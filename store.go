package notify

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by single-record reads when nothing matches.
var ErrNotFound = errors.New("notify: not found")

// ErrNotificationExists answers CreateNotification of a notification already stored: one whose
// IdempotencyKey its (Partition, Subject) has already used, or whose ID is already stored under the same
// (Partition, Subject). The notification passed in is filled with the stored one and nothing is written.
var ErrNotificationExists = errors.New("notify: the notification is already stored")

// ErrNotificationIDTaken answers CreateNotification of an ID already stored under another partition or
// subject; nothing is written and nothing of the stored notification is answered.
var ErrNotificationIDTaken = errors.New("notify: the notification id is taken")

// Store is the persistence contract: notifications, push device tokens, and channel preferences. The
// Partition is opaque and the store filters on it, never interprets it; an empty partition ("") on a
// partition-scoped read matches NOTHING — the predicates read (? <> ” AND partition = ?), so an
// unscoped read returns no rows rather than every partition's. A blank partition that matched everything
// would read another partition's notifications and device tokens. There is no unscoped read; a caller
// that must cross partitions names the partition it means. A consumer supplies a *sql.DB store
// (the shipped default), a mesh-RPC gateway, or the reference MemStore.
type Store interface {
	// ── Notifications ──
	// CreateNotification stores n, minting ID and CreatedAt when unset. When n is already stored (a
	// non-empty n.IdempotencyKey already used by n's (Partition, Subject), or n.ID already stored under
	// them) it stores nothing, fills n with the stored notification (its IdempotencyKey kept) and answers
	// ErrNotificationExists; an ID stored under another partition or subject answers
	// ErrNotificationIDTaken.
	CreateNotification(ctx context.Context, n *Notification) error
	GetNotification(ctx context.Context, partition, subject, id string) (*Notification, error)
	// ListNotifications returns a subject's notifications, newest first, optionally unread-only and
	// filtered by category (empty = all), paged by an opaque cursor; returns the page + the next cursor
	// ("" when exhausted).
	ListNotifications(ctx context.Context, partition, subject, category string, unreadOnly bool, limit int, cursor string) (page []*Notification, next string, err error)
	ListNotificationsSince(ctx context.Context, partition, subject string, since time.Time) ([]*Notification, error)
	// UnreadCount returns the count of unread, non-dismissed notifications and the newest such time.
	UnreadCount(ctx context.Context, partition, subject string) (count int, latest *time.Time, err error)
	MarkRead(ctx context.Context, partition, subject string, ids []string) (int, error)
	MarkAllRead(ctx context.Context, partition, subject string) (int, error)
	Dismiss(ctx context.Context, partition, subject, id string) error
	// SetDeliveredVia records the opaque channels a notification was delivered on (the Notifier writes it).
	SetDeliveredVia(ctx context.Context, id string, channels []string) error

	// ── Push device tokens ──
	RegisterToken(ctx context.Context, t *DeviceToken) error
	ListActiveTokens(ctx context.Context, partition, subject string) ([]*DeviceToken, error)
	InvalidateToken(ctx context.Context, partition, token string, at int64) error
	// CleanupStaleTokens deletes tokens last seen (or created) before `before`; returns the count removed.
	CleanupStaleTokens(ctx context.Context, before int64) (int, error)

	// ── Preferences ──
	GetPreferences(ctx context.Context, partition, subject string) ([]*Preference, error)
	SetPreference(ctx context.Context, p *Preference) error

	// ── User events (idempotent publish anchor) ──
	// RecordUserEvent inserts a user-event dedup row. When IdempotencyKey is
	// non-empty and the (Partition, Subject, IdempotencyKey) tuple already exists,
	// it returns the prior row + ErrUserEventAlreadyPublished (an idempotent
	// replay — surface the cached outcome). An empty key never dedups. It mints
	// EventID/CreatedAt when unset and returns the stored row.
	RecordUserEvent(ctx context.Context, e *UserEvent) (*UserEvent, error)
	// UpdateUserEventOutcome records the post-fan-out result (delivered,
	// subscriber count, channels actually attempted) on an existing row;
	// ErrNotFound when the event id is unknown.
	UpdateUserEventOutcome(ctx context.Context, eventID string, delivered bool, subscriberCount int32, channels []string) error
	// GetUserEventByIdempotency reads a user event by its dedup tuple; ErrNotFound
	// when absent.
	GetUserEventByIdempotency(ctx context.Context, partition, subject, idempotencyKey string) (*UserEvent, error)
}
