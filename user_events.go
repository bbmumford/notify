package notify

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// UserEvent is the persisted anchor of a published user event — the idempotency
// record a publish writes before fan-out. The (Partition, UserID, IdempotencyKey)
// tuple dedups a retried publish: a repeat with the same NON-EMPTY key returns the
// stored row + ErrUserEventAlreadyPublished instead of fanning again, so
// at-least-once delivery is safe and observable. Channels + the delivery outcome
// are recorded before fan-out so a crash-then-retry resumes from the row rather
// than re-fanning; UpdateUserEventOutcome writes the result after. Partition is the
// opaque partition-scope; an empty IdempotencyKey never dedups (every call a fresh row).
type UserEvent struct {
	EventID         string
	Partition       string
	UserID          string
	EventType       string
	SourceDomain    string
	IdempotencyKey  string
	Title           string
	Body            string
	ActionURL       string
	PayloadJSON     string
	Channels        []string
	Delivered       bool
	SubscriberCount int32
	CreatedAt       int64 // Unix seconds
}

// ErrUserEventAlreadyPublished is returned by RecordUserEvent when the
// (partition, user_id, idempotency_key) tuple already exists. A caller MUST treat
// it as an idempotent replay and surface the returned row's cached outcome.
var ErrUserEventAlreadyPublished = errors.New("notify: user event already published")

func cloneUserEvent(e *UserEvent) *UserEvent {
	cp := *e
	if e.Channels != nil {
		cp.Channels = append([]string(nil), e.Channels...)
	}
	return &cp
}

// ── MemStore ──

func (m *MemStore) RecordUserEvent(_ context.Context, e *UserEvent) (*UserEvent, error) {
	if e.Partition == "" || e.UserID == "" || e.EventType == "" {
		return nil, errors.New("notify: partition, user_id, event_type are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if e.IdempotencyKey != "" {
		for _, ex := range m.events {
			if ex.Partition == e.Partition && ex.UserID == e.UserID && ex.IdempotencyKey == e.IdempotencyKey {
				return cloneUserEvent(ex), ErrUserEventAlreadyPublished
			}
		}
	}
	stored := cloneUserEvent(e)
	if stored.EventID == "" {
		stored.EventID = m.newID()
	}
	if stored.CreatedAt == 0 {
		stored.CreatedAt = m.now().Unix()
	}
	m.events[stored.EventID] = stored
	e.EventID, e.CreatedAt = stored.EventID, stored.CreatedAt
	return cloneUserEvent(stored), nil
}

func (m *MemStore) UpdateUserEventOutcome(_ context.Context, eventID string, delivered bool, subscriberCount int32, channels []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ev, ok := m.events[eventID]
	if !ok {
		return ErrNotFound
	}
	ev.Delivered = delivered
	ev.SubscriberCount = subscriberCount
	ev.Channels = append([]string(nil), channels...)
	return nil
}

func (m *MemStore) GetUserEventByIdempotency(_ context.Context, partition, userID, idempotencyKey string) (*UserEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ex := range m.events {
		if ex.Partition == partition && ex.UserID == userID && ex.IdempotencyKey == idempotencyKey {
			return cloneUserEvent(ex), nil
		}
	}
	return nil, ErrNotFound
}

// ── SQLStore ──

func (s *SQLStore) RecordUserEvent(ctx context.Context, e *UserEvent) (*UserEvent, error) {
	if e.Partition == "" || e.UserID == "" || e.EventType == "" {
		return nil, errors.New("notify: partition, user_id, event_type are required")
	}
	if e.IdempotencyKey != "" {
		existing, err := s.GetUserEventByIdempotency(ctx, e.Partition, e.UserID, e.IdempotencyKey)
		if err == nil {
			return existing, ErrUserEventAlreadyPublished
		} else if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}
	if e.EventID == "" {
		e.EventID = randomID()
	}
	if e.CreatedAt == 0 {
		e.CreatedAt = time.Now().Unix()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO notify_user_events
		(event_id, partition, user_id, event_type, source_domain, idempotency_key,
		 title, body, action_url, payload_json, channels, delivered, subscriber_count, created_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.EventID, e.Partition, e.UserID, e.EventType, e.SourceDomain, e.IdempotencyKey,
		e.Title, e.Body, e.ActionURL, e.PayloadJSON, jsonSliceS(e.Channels),
		boolInt(e.Delivered), e.SubscriberCount, e.CreatedAt)
	if err != nil {
		// Race: a concurrent insert won the unique (partition,user,idempotency_key).
		// Return the winner's row as an idempotent replay rather than the raw error.
		if e.IdempotencyKey != "" {
			if winner, gerr := s.GetUserEventByIdempotency(ctx, e.Partition, e.UserID, e.IdempotencyKey); gerr == nil {
				return winner, ErrUserEventAlreadyPublished
			}
		}
		return nil, err
	}
	return cloneUserEvent(e), nil
}

func (s *SQLStore) UpdateUserEventOutcome(ctx context.Context, eventID string, delivered bool, subscriberCount int32, channels []string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE notify_user_events
		SET delivered = ?, subscriber_count = ?, channels = ? WHERE event_id = ?`,
		boolInt(delivered), subscriberCount, jsonSliceS(channels), eventID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLStore) GetUserEventByIdempotency(ctx context.Context, partition, userID, idempotencyKey string) (*UserEvent, error) {
	var (
		e            UserEvent
		channels     string
		deliveredInt int
	)
	err := s.db.QueryRowContext(ctx, `SELECT
		event_id, partition, user_id, event_type, source_domain, idempotency_key,
		title, body, action_url, payload_json, channels, delivered, subscriber_count, created_at
		FROM notify_user_events WHERE partition = ? AND user_id = ? AND idempotency_key = ? LIMIT 1`,
		partition, userID, idempotencyKey).Scan(
		&e.EventID, &e.Partition, &e.UserID, &e.EventType, &e.SourceDomain, &e.IdempotencyKey,
		&e.Title, &e.Body, &e.ActionURL, &e.PayloadJSON, &channels, &deliveredInt, &e.SubscriberCount, &e.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	e.Delivered = deliveredInt != 0
	e.Channels = readSliceS(channels)
	return &e, nil
}
