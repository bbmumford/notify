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
// store filters on but never interprets ("" matches any partition). Times persist as INTEGER Unix nanos with
// NULL for an unset *time.Time; metadata/delivered_via/channels persist as JSON.
type SQLStore struct{ db *sql.DB }

// NewSQLStore builds a store over an opened *sql.DB. Call InitSchema once before use.
func NewSQLStore(db *sql.DB) *SQLStore { return &SQLStore{db: db} }

var _ Store = (*SQLStore)(nil)

// InitSchema creates the tables + indexes (idempotent); run once at boot.
func (s *SQLStore) InitSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS notify_notifications (
			id TEXT PRIMARY KEY, partition TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL DEFAULT '',
			category TEXT NOT NULL DEFAULT '', title TEXT NOT NULL DEFAULT '', body TEXT NOT NULL DEFAULT '',
			priority TEXT NOT NULL DEFAULT '', action_url TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT '', source_entity TEXT NOT NULL DEFAULT '',
			metadata TEXT NOT NULL DEFAULT '{}', read INTEGER NOT NULL DEFAULT 0,
			dismissed INTEGER NOT NULL DEFAULT 0, delivered_via TEXT NOT NULL DEFAULT '[]',
			created_at INTEGER NOT NULL DEFAULT 0, read_at INTEGER, dismissed_at INTEGER
		)`,
		`CREATE INDEX IF NOT EXISTS idx_notify_notifs_user ON notify_notifications(partition, user_id, created_at)`,
		`CREATE TABLE IF NOT EXISTS notify_device_tokens (
			id TEXT PRIMARY KEY, partition TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL DEFAULT '',
			token TEXT NOT NULL DEFAULT '', platform TEXT NOT NULL DEFAULT '', provider TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL DEFAULT 0, last_seen_at INTEGER, invalidated_at INTEGER
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_notify_tokens_token ON notify_device_tokens(token)`,
		`CREATE INDEX IF NOT EXISTS idx_notify_tokens_user ON notify_device_tokens(partition, user_id)`,
		`CREATE TABLE IF NOT EXISTS notify_preferences (
			partition TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL DEFAULT '', category TEXT NOT NULL DEFAULT '',
			channels TEXT NOT NULL DEFAULT '{}', updated_at INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (partition, user_id, category)
		)`,
		`CREATE TABLE IF NOT EXISTS notify_user_events (
			event_id TEXT PRIMARY KEY, partition TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL DEFAULT '',
			event_type TEXT NOT NULL DEFAULT '', source_domain TEXT NOT NULL DEFAULT '',
			idempotency_key TEXT NOT NULL DEFAULT '', title TEXT NOT NULL DEFAULT '',
			body TEXT NOT NULL DEFAULT '', action_url TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}', channels TEXT NOT NULL DEFAULT '[]',
			delivered INTEGER NOT NULL DEFAULT 0, subscriber_count INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL DEFAULT 0
		)`,
		// Dedup only for a non-empty idempotency key: an empty key writes a fresh row every call.
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_notify_user_events_idem ON notify_user_events(partition, user_id, idempotency_key) WHERE idempotency_key != ''`,
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

const notifCols = `id, partition, user_id, category, title, body, priority, action_url, source,
	source_entity, metadata, read, dismissed, delivered_via, created_at, read_at, dismissed_at`

func scanNotif(sc interface{ Scan(...any) error }) (*Notification, error) {
	var n Notification
	var meta, delivered string
	var read, dismissed int
	var created int64
	var readAt, dismissedAt sql.NullInt64
	if err := sc.Scan(&n.ID, &n.Partition, &n.UserID, &n.Category, &n.Title, &n.Body, &n.Priority,
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
	_, err := s.db.ExecContext(ctx, `INSERT INTO notify_notifications (`+notifCols+`)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		n.ID, n.Partition, n.UserID, n.Category, n.Title, n.Body, n.Priority, n.ActionURL, n.Source,
		n.SourceEntity, jsonStrMapS(n.Metadata), boolInt(n.Read), boolInt(n.Dismissed),
		jsonSliceS(n.DeliveredVia), tnanos(n.CreatedAt), ptrNanos(n.ReadAt), ptrNanos(n.DismissedAt))
	return err
}

func (s *SQLStore) GetNotification(ctx context.Context, partition, userID, id string) (*Notification, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+notifCols+` FROM notify_notifications
		WHERE id = ? AND (? <> '' AND partition = ?) AND (? = '' OR user_id = ?)`, id, partition, partition, userID, userID)
	n, err := scanNotif(row)
	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	return n, err
}

func (s *SQLStore) ListNotifications(ctx context.Context, partition, userID, category string, unreadOnly bool, limit int, cursor string) ([]*Notification, string, error) {
	where := `(? <> '' AND partition = ?) AND user_id = ? AND dismissed = 0 AND (? = '' OR category = ?)`
	args := []any{partition, partition, userID, category, category}
	if unreadOnly {
		where += ` AND read = 0`
	}
	offset := 0
	if cursor != "" {
		if v, err := strconv.Atoi(cursor); err == nil && v > 0 {
			offset = v
		}
	}
	q := `SELECT ` + notifCols + ` FROM notify_notifications WHERE ` + where + ` ORDER BY created_at DESC, id DESC`
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

func (s *SQLStore) ListNotificationsSince(ctx context.Context, partition, userID string, since time.Time) ([]*Notification, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+notifCols+` FROM notify_notifications
		WHERE (? <> '' AND partition = ?) AND user_id = ? AND dismissed = 0 AND created_at > ?
		ORDER BY created_at DESC, id DESC`, partition, partition, userID, since.UnixNano())
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

func (s *SQLStore) UnreadCount(ctx context.Context, partition, userID string) (int, *time.Time, error) {
	var count int
	var latest sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), MAX(created_at) FROM notify_notifications
		WHERE (? <> '' AND partition = ?) AND user_id = ? AND read = 0 AND dismissed = 0`,
		partition, partition, userID).Scan(&count, &latest)
	if err != nil {
		return 0, nil, err
	}
	return count, fromNullNanos(latest), nil
}

func (s *SQLStore) MarkRead(ctx context.Context, partition, userID string, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := []any{time.Now().UnixNano(), partition, partition, userID}
	for _, id := range ids {
		args = append(args, id)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE notify_notifications SET read = 1, read_at = ?
		WHERE (? <> '' AND partition = ?) AND user_id = ? AND read = 0 AND id IN (`+ph+`)`, args...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *SQLStore) MarkAllRead(ctx context.Context, partition, userID string) (int, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE notify_notifications SET read = 1, read_at = ?
		WHERE (? <> '' AND partition = ?) AND user_id = ? AND read = 0 AND dismissed = 0`,
		time.Now().UnixNano(), partition, partition, userID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *SQLStore) Dismiss(ctx context.Context, partition, userID, id string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE notify_notifications SET dismissed = 1,
		dismissed_at = COALESCE(dismissed_at, ?) WHERE id = ? AND (? <> '' AND partition = ?) AND user_id = ?`,
		time.Now().UnixNano(), id, partition, partition, userID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLStore) SetDeliveredVia(ctx context.Context, id string, channels []string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE notify_notifications SET delivered_via = ? WHERE id = ?`,
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
	// Token is unique. A FIRST registration leaves last_seen_at NULL (cleanup then falls back to
	// created_at, matching MemStore); a RE-register refreshes partition/user/platform + last_seen and clears
	// invalidation. The update's last_seen_at is a bound parameter, not excluded (which is NULL here).
	_, err := s.db.ExecContext(ctx, `INSERT INTO notify_device_tokens
		(id, partition, user_id, token, platform, provider, created_at, last_seen_at, invalidated_at)
		VALUES (?,?,?,?,?,?,?,NULL,NULL)
		ON CONFLICT(token) DO UPDATE SET partition=excluded.partition, user_id=excluded.user_id,
			platform=excluded.platform, provider=excluded.provider, last_seen_at=?, invalidated_at=NULL`,
		t.ID, t.Partition, t.UserID, t.Token, t.Platform, t.Provider, tnanos(t.CreatedAt), now)
	return err
}

func (s *SQLStore) ListActiveTokens(ctx context.Context, partition, userID string) ([]*DeviceToken, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, partition, user_id, token, platform, provider,
		created_at, last_seen_at, invalidated_at FROM notify_device_tokens
		WHERE (? <> '' AND partition = ?) AND user_id = ? AND invalidated_at IS NULL ORDER BY created_at ASC`,
		partition, partition, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*DeviceToken
	for rows.Next() {
		var t DeviceToken
		var created int64
		var lastSeen, invalidated sql.NullInt64
		if err := rows.Scan(&t.ID, &t.Partition, &t.UserID, &t.Token, &t.Platform, &t.Provider,
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
	res, err := s.db.ExecContext(ctx, `UPDATE notify_device_tokens
		SET invalidated_at = COALESCE(invalidated_at, ?) WHERE token = ? AND (? <> '' AND partition = ?)`,
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
	res, err := s.db.ExecContext(ctx, `DELETE FROM notify_device_tokens
		WHERE COALESCE(last_seen_at, created_at) < ?`, before)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ── Preferences ──

func (s *SQLStore) GetPreferences(ctx context.Context, partition, userID string) ([]*Preference, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT partition, user_id, category, channels, updated_at
		FROM notify_preferences WHERE (? <> '' AND partition = ?) AND user_id = ? ORDER BY category ASC`,
		partition, partition, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Preference
	for rows.Next() {
		var p Preference
		var channels string
		var updated int64
		if err := rows.Scan(&p.Partition, &p.UserID, &p.Category, &channels, &updated); err != nil {
			return nil, err
		}
		p.Channels = readBoolMapS(channels)
		p.UpdatedAt = fromNanos(updated)
		out = append(out, &p)
	}
	return out, rows.Err()
}

func (s *SQLStore) SetPreference(ctx context.Context, p *Preference) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO notify_preferences (partition, user_id, category, channels, updated_at)
		VALUES (?,?,?,?,?)
		ON CONFLICT(partition, user_id, category) DO UPDATE SET channels=excluded.channels, updated_at=excluded.updated_at`,
		p.Partition, p.UserID, p.Category, jsonBoolMapS(p.Channels), time.Now().UnixNano())
	return err
}
