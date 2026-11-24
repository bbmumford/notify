package notify

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// SQLStore is the durable default Store over any database/sql driver — the shipped standalone store. The
// package imports only database/sql (stdlib); the APP supplies the opened *sql.DB and registers the
// driver, so notify pulls no driver dependency yet works out of the box. Partition is an opaque column the
// store filters on but never interprets ("" matches nothing: the predicates read
// (? <> ” AND partition = ?)). Times persist as INTEGER Unix nanos with
// NULL for an unset *time.Time; metadata/deliveredVia/channels persist as JSON.
type SQLStore struct{ db *sql.DB }

// NewSQLStore builds a store over an opened *sql.DB. Call InitSchema once before use.
func NewSQLStore(db *sql.DB) *SQLStore { return &SQLStore{db: db} }

var _ Store = (*SQLStore)(nil)

// InitSchema creates the tables + indexes (idempotent); run once at boot.
func (s *SQLStore) InitSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS notifyNotifications (
			id TEXT PRIMARY KEY, partition TEXT NOT NULL DEFAULT '', subject TEXT NOT NULL DEFAULT '',
			category TEXT NOT NULL DEFAULT '', title TEXT NOT NULL DEFAULT '', body TEXT NOT NULL DEFAULT '',
			priority TEXT NOT NULL DEFAULT '', actionUrl TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT '', sourceEntity TEXT NOT NULL DEFAULT '',
			metadata TEXT NOT NULL DEFAULT '{}', read INTEGER NOT NULL DEFAULT 0,
			dismissed INTEGER NOT NULL DEFAULT 0, deliveredVia TEXT NOT NULL DEFAULT '[]',
			createdAt INTEGER NOT NULL DEFAULT 0, readAt INTEGER, dismissedAt INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS idxNotifyNotifsSubject ON notifyNotifications(partition, subject, createdAt)`,
		// One row per notification created with an idempotency key; its primary key is the dedup tuple.
		`CREATE TABLE IF NOT EXISTS notifyNotificationKeys (
			partition TEXT NOT NULL DEFAULT '', subject TEXT NOT NULL DEFAULT '',
			idempotencyKey TEXT NOT NULL, notificationId TEXT NOT NULL,
			PRIMARY KEY (partition, subject, idempotencyKey)
		)`,
		`CREATE TABLE IF NOT EXISTS notifyDeviceTokens (
			id TEXT PRIMARY KEY, partition TEXT NOT NULL DEFAULT '', subject TEXT NOT NULL DEFAULT '',
			token TEXT NOT NULL DEFAULT '', platform TEXT NOT NULL DEFAULT '', provider TEXT NOT NULL DEFAULT '',
			createdAt INTEGER NOT NULL DEFAULT 0, lastSeenAt INTEGER, invalidatedAt INTEGER
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idxNotifyTokensToken ON notifyDeviceTokens(token)`,
		`CREATE INDEX IF NOT EXISTS idxNotifyTokensSubject ON notifyDeviceTokens(partition, subject)`,
		`CREATE TABLE IF NOT EXISTS notifyPreferences (
			partition TEXT NOT NULL DEFAULT '', subject TEXT NOT NULL DEFAULT '', category TEXT NOT NULL DEFAULT '',
			channels TEXT NOT NULL DEFAULT '{}', updatedAt INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (partition, subject, category)
		)`,
		`CREATE TABLE IF NOT EXISTS notifyUserEvents (
			eventId TEXT PRIMARY KEY, partition TEXT NOT NULL DEFAULT '', subject TEXT NOT NULL DEFAULT '',
			eventType TEXT NOT NULL DEFAULT '', sourceDomain TEXT NOT NULL DEFAULT '',
			idempotencyKey TEXT NOT NULL DEFAULT '', title TEXT NOT NULL DEFAULT '',
			body TEXT NOT NULL DEFAULT '', actionUrl TEXT NOT NULL DEFAULT '',
			payloadJson TEXT NOT NULL DEFAULT '{}', channels TEXT NOT NULL DEFAULT '[]',
			delivered INTEGER NOT NULL DEFAULT 0, subscriberCount INTEGER NOT NULL DEFAULT 0,
			createdAt INTEGER NOT NULL DEFAULT 0
		)`,
		// Dedup only for a non-empty idempotency key: an empty key writes a fresh row every call.
		`CREATE UNIQUE INDEX IF NOT EXISTS idxNotifyUserEventsIdem ON notifyUserEvents(partition, subject, idempotencyKey) WHERE idempotencyKey != ''`,
	}
	for _, q := range stmts {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// ── codecs ──

func tnanos(t time.Time) int64    { return t.UnixNano() }
func fromNanos(n int64) time.Time { return time.Unix(0, n).UTC() }

func ptrNanos(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UnixNano()
}

func fromNullNanos(n sql.NullInt64) *time.Time {
	if !n.Valid {
		return nil
	}
	t := fromNanos(n.Int64)
	return &t
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func jsonStrMapS(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func readStrMapS(s string) map[string]string {
	if s == "" || s == "{}" {
		return nil
	}
	var m map[string]string
	_ = json.Unmarshal([]byte(s), &m)
	return m
}

func jsonBoolMapS(m map[string]bool) string {
	if len(m) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func readBoolMapS(s string) map[string]bool {
	if s == "" || s == "{}" {
		return nil
	}
	var m map[string]bool
	_ = json.Unmarshal([]byte(s), &m)
	return m
}

func jsonSliceS(v []string) string {
	if len(v) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func readSliceS(s string) []string {
	if s == "" || s == "[]" {
		return nil
	}
	var v []string
	_ = json.Unmarshal([]byte(s), &v)
	return v
}

// ── Notifications ──

const notifCols = `id, partition, subject, category, title, body, priority, actionUrl, source,
	sourceEntity, metadata, read, dismissed, deliveredVia, createdAt, readAt, dismissedAt`

func scanNotif(sc interface{ Scan(...any) error }) (*Notification, error) {
	var n Notification
	var meta, delivered string
	var read, dismissed int
	var created int64
	var readAt, dismissedAt sql.NullInt64
	if err := sc.Scan(&n.ID, &n.Partition, &n.Subject, &n.Category, &n.Title, &n.Body, &n.Priority,
		&n.ActionURL, &n.Source, &n.SourceEntity, &meta, &read, &dismissed, &delivered, &created,
		&readAt, &dismissedAt); err != nil {
		return nil, err
	}
	n.Metadata = readStrMapS(meta)
	n.DeliveredVia = readSliceS(delivered)
	n.Read = read != 0
	n.Dismissed = dismissed != 0
	n.CreatedAt = fromNanos(created)
	n.ReadAt = fromNullNanos(readAt)
	n.DismissedAt = fromNullNanos(dismissedAt)
	return &n, nil
}

func (s *SQLStore) CreateNotification(ctx context.Context, n *Notification) error {
	if n.ID == "" {
		n.ID = randomID()
	}
	if n.CreatedAt.IsZero() {
		n.CreatedAt = time.Now()
	}
	// The key row and the notification commit together, so a key is never held by a notification that
	// was not stored, and a concurrent create with the same key finds the first one's row.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if n.IdempotencyKey != "" {
		res, err := tx.ExecContext(ctx, `INSERT INTO notifyNotificationKeys (partition, subject, idempotencyKey, notificationId)
			VALUES (?,?,?,?) ON CONFLICT(partition, subject, idempotencyKey) DO NOTHING`,
			n.Partition, n.Subject, n.IdempotencyKey, n.ID)
		if err != nil {
			return err
		}
		if added, err := res.RowsAffected(); err != nil {
			return err
		} else if added == 0 {
			var id string
			if err := tx.QueryRowContext(ctx, `SELECT notificationId FROM notifyNotificationKeys
				WHERE partition = ? AND subject = ? AND idempotencyKey = ?`,
				n.Partition, n.Subject, n.IdempotencyKey).Scan(&id); err != nil {
				return err
			}
			prior, err := scanNotif(tx.QueryRowContext(ctx, `SELECT `+notifCols+` FROM notifyNotifications WHERE id = ?`, id))
			if err != nil {
				return err
			}
			key := n.IdempotencyKey
			*n = *prior
			n.IdempotencyKey = key
			return ErrNotificationExists
		}
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO notifyNotifications (`+notifCols+`)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING`,
		n.ID, n.Partition, n.Subject, n.Category, n.Title, n.Body, n.Priority, n.ActionURL, n.Source,
		n.SourceEntity, jsonStrMapS(n.Metadata), boolInt(n.Read), boolInt(n.Dismissed),
		jsonSliceS(n.DeliveredVia), tnanos(n.CreatedAt), ptrNanos(n.ReadAt), ptrNanos(n.DismissedAt))
	if err != nil {
		return err
	}
	if added, err := res.RowsAffected(); err != nil {
		return err
	} else if added == 0 {
		prior, err := scanNotif(tx.QueryRowContext(ctx, `SELECT `+notifCols+` FROM notifyNotifications WHERE id = ?`, n.ID))
		if err != nil {
			return err
		}
		if prior.Partition != n.Partition || prior.Subject != n.Subject {
			return ErrNotificationIDTaken
		}
		key := n.IdempotencyKey
		*n = *prior
		n.IdempotencyKey = key
		return ErrNotificationExists
	}
	return tx.Commit()
}

func (s *SQLStore) GetNotification(ctx context.Context, partition, subject, id string) (*Notification, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+notifCols+` FROM notifyNotifications
		WHERE id = ? AND (? <> '' AND partition = ?) AND (? = '' OR subject = ?)`, id, partition, partition, subject, subject)
	n, err := scanNotif(row)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	return n, err
}

func (s *SQLStore) ListNotifications(ctx context.Context, partition, subject, category string, unreadOnly bool, limit int, cursor string) ([]*Notification, string, error) {
	where := `(? <> '' AND partition = ?) AND subject = ? AND dismissed = 0 AND (? = '' OR category = ?)`
	args := []any{partition, partition, subject, category, category}
	if unreadOnly {
		where += ` AND read = 0`
	}
	offset := 0
	if cursor != "" {
		if v, err := strconv.Atoi(cursor); err == nil && v > 0 {
			offset = v
		}
	}
	q := `SELECT ` + notifCols + ` FROM notifyNotifications WHERE ` + where + ` ORDER BY createdAt DESC, id DESC`
	fetch := limit
	if limit > 0 {
		fetch = limit + 1 // over-fetch one to know whether a next page exists
		q += ` LIMIT ? OFFSET ?`
		args = append(args, fetch, offset)
	} else if offset > 0 {
		q += ` LIMIT -1 OFFSET ?`
		args = append(args, offset)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []*Notification
	for rows.Next() {
		n, err := scanNotif(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		next = strconv.Itoa(offset + limit)
	}
	return out, next, nil
}

func (s *SQLStore) ListNotificationsSince(ctx context.Context, partition, subject string, since time.Time) ([]*Notification, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+notifCols+` FROM notifyNotifications
		WHERE (? <> '' AND partition = ?) AND subject = ? AND dismissed = 0 AND createdAt > ?
		ORDER BY createdAt DESC, id DESC`, partition, partition, subject, since.UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Notification
	for rows.Next() {
		n, err := scanNotif(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *SQLStore) UnreadCount(ctx context.Context, partition, subject string) (int, *time.Time, error) {
	var count int
	var latest sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), MAX(createdAt) FROM notifyNotifications
		WHERE (? <> '' AND partition = ?) AND subject = ? AND read = 0 AND dismissed = 0`,
		partition, partition, subject).Scan(&count, &latest)
	if err != nil {
		return 0, nil, err
	}
	return count, fromNullNanos(latest), nil
}

func (s *SQLStore) MarkRead(ctx context.Context, partition, subject string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := []any{time.Now().UnixNano(), partition, partition, subject}
	for _, id := range ids {
		args = append(args, id)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE notifyNotifications SET read = 1, readAt = ?
		WHERE (? <> '' AND partition = ?) AND subject = ? AND read = 0 AND id IN (`+ph+`)`, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *SQLStore) MarkAllRead(ctx context.Context, partition, subject string) (int, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE notifyNotifications SET read = 1, readAt = ?
		WHERE (? <> '' AND partition = ?) AND subject = ? AND read = 0 AND dismissed = 0`,
		time.Now().UnixNano(), partition, partition, subject)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *SQLStore) Dismiss(ctx context.Context, partition, subject, id string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE notifyNotifications SET dismissed = 1,
		dismissedAt = COALESCE(dismissedAt, ?) WHERE id = ? AND (? <> '' AND partition = ?) AND subject = ?`,
		time.Now().UnixNano(), id, partition, partition, subject)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLStore) SetDeliveredVia(ctx context.Context, id string, channels []string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE notifyNotifications SET deliveredVia = ? WHERE id = ?`,
		jsonSliceS(channels), id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

// ── Push device tokens ──

func (s *SQLStore) RegisterToken(ctx context.Context, t *DeviceToken) error {
	if t.ID == "" {
		t.ID = randomID()
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	now := time.Now().UnixNano()
	// Token is unique. A FIRST registration leaves lastSeenAt NULL (cleanup then falls back to
	// createdAt, matching MemStore); a RE-register refreshes partition/subject/platform + last_seen and clears
	// invalidation. The update's lastSeenAt is a bound parameter, not excluded (which is NULL here).
	_, err := s.db.ExecContext(ctx, `INSERT INTO notifyDeviceTokens
		(id, partition, subject, token, platform, provider, createdAt, lastSeenAt, invalidatedAt)
		VALUES (?,?,?,?,?,?,?,NULL,NULL)
		ON CONFLICT(token) DO UPDATE SET partition=excluded.partition, subject=excluded.subject,
			platform=excluded.platform, provider=excluded.provider, lastSeenAt=?, invalidatedAt=NULL`,
		t.ID, t.Partition, t.Subject, t.Token, t.Platform, t.Provider, tnanos(t.CreatedAt), now)
	return err
}

func (s *SQLStore) ListActiveTokens(ctx context.Context, partition, subject string) ([]*DeviceToken, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, partition, subject, token, platform, provider,
		createdAt, lastSeenAt, invalidatedAt FROM notifyDeviceTokens
		WHERE (? <> '' AND partition = ?) AND subject = ? AND invalidatedAt IS NULL ORDER BY createdAt ASC`,
		partition, partition, subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DeviceToken
	for rows.Next() {
		var t DeviceToken
		var created int64
		var lastSeen, invalidated sql.NullInt64
		if err := rows.Scan(&t.ID, &t.Partition, &t.Subject, &t.Token, &t.Platform, &t.Provider,
			&created, &lastSeen, &invalidated); err != nil {
			return nil, err
		}
		t.CreatedAt = fromNanos(created)
		t.LastSeenAt = fromNullNanos(lastSeen)
		t.InvalidatedAt = fromNullNanos(invalidated)
		out = append(out, &t)
	}
	return out, rows.Err()
}

func (s *SQLStore) InvalidateToken(ctx context.Context, partition, token string, at int64) error {
	res, err := s.db.ExecContext(ctx, `UPDATE notifyDeviceTokens
		SET invalidatedAt = COALESCE(invalidatedAt, ?) WHERE token = ? AND (? <> '' AND partition = ?)`,
		at, token, partition, partition)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLStore) CleanupStaleTokens(ctx context.Context, before int64) (int, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM notifyDeviceTokens
		WHERE COALESCE(lastSeenAt, createdAt) < ?`, before)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ── Preferences ──

func (s *SQLStore) GetPreferences(ctx context.Context, partition, subject string) ([]*Preference, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT partition, subject, category, channels, updatedAt
		FROM notifyPreferences WHERE (? <> '' AND partition = ?) AND subject = ? ORDER BY category ASC`,
		partition, partition, subject)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Preference
	for rows.Next() {
		var p Preference
		var channels string
		var updated int64
		if err := rows.Scan(&p.Partition, &p.Subject, &p.Category, &channels, &updated); err != nil {
			return nil, err
		}
		p.Channels = readBoolMapS(channels)
		p.UpdatedAt = fromNanos(updated)
		out = append(out, &p)
	}
	return out, rows.Err()
}

func (s *SQLStore) SetPreference(ctx context.Context, p *Preference) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO notifyPreferences (partition, subject, category, channels, updatedAt)
		VALUES (?,?,?,?,?)
		ON CONFLICT(partition, subject, category) DO UPDATE SET channels=excluded.channels, updatedAt=excluded.updatedAt`,
		p.Partition, p.Subject, p.Category, jsonBoolMapS(p.Channels), time.Now().UnixNano())
	return err
}
