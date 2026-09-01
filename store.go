package notify

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by single-record reads when nothing matches.
var ErrNotFound = errors.New("notify: not found")

// Store is the persistence contract: notifications, push device tokens, and channel preferences. The
// Partition is opaque and the store filters on it, never interprets it; an empty partition ("") on an
// partition-scoped read means "any partition" (the admin/all-scope view). A consumer supplies a *sql.DB store
// (the shipped default), a mesh-RPC gateway, or the reference MemStore.
type Store interface {
	// ── Notifications ──
	CreateNotification(ctx context.Context, n *Notification) error
	GetNotification(ctx context.Context, partition, userID, id string) (*Notification, error)
	// ListNotifications returns a user's notifications, newest first, optionally unread-only and
	// filtered by category (empty = all), paged by an opaque cursor; returns the page + the next cursor
	// ("" when exhausted).
	ListNotifications(ctx context.Context, partition, userID, category string, unreadOnly bool, limit int, cursor string) (page []*Notification, next string, err error)
	ListNotificationsSince(ctx context.Context, partition, userID string, since time.Time) ([]*Notification, error)
	// UnreadCount returns the count of unread, non-dismissed notifications and the newest such time.
	UnreadCount(ctx context.Context, partition, userID string) (count int, latest *time.Time, err error)
	MarkRead(ctx context.Context, partition, userID string, ids []string) (int, error)
	MarkAllRead(ctx context.Context, partition, userID string) (int, error)
	Dismiss(ctx context.Context, partition, userID, id string) error
	// SetDeliveredVia records the opaque channels a notification was delivered on (the Notifier writes it).
	SetDeliveredVia(ctx context.Context, id string, channels []string) error

	// ── Push device tokens ──
	RegisterToken(ctx context.Context, t *DeviceToken) error
	ListActiveTokens(ctx context.Context, partition, userID string) ([]*DeviceToken, error)
	InvalidateToken(ctx context.Context, partition, token string, at int64) error
	// CleanupStaleTokens deletes tokens last seen (or created) before `before`; returns the count removed.
	CleanupStaleTokens(ctx context.Context, before int64) (int, error)

	// ── Preferences ──
	GetPreferences(ctx context.Context, partition, userID string) ([]*Preference, error)
	SetPreference(ctx context.Context, p *Preference) error
}
