// Package notify is the scope-agnostic notification system. It owns in-app notifications, per-subject
// push device tokens, and per-category channel preferences over an injected store, and fans a
// notification out to pluggable delivery Sinks. The primary axis is an opaque Partition the system
// never interprets; the Subject is opaque too; and delivery channels
// are opaque strings a consumer registers a Sink for ("in_app", "email", "push", "sms", "webhook", or
// anything else) — so the channel set is extensible, not a fixed enum. Times are time.Time; a nil
// *time.Time means "not yet" (unread / not dismissed / not invalidated).
package notify

import "time"

// Notification is a single notification addressed to a subject. Partition is the opaque partition-scope; Metadata
// is opaque JSON a consumer attaches; DeliveredVia records the opaque channel names it was delivered on.
type Notification struct {
	ID           string
	Partition    string // opaque partition-scope
	Subject      string
	Category     string
	Title        string
	Body         string
	Priority     string
	ActionURL    string
	Source       string // originating service/component
	SourceEntity string // opaque id of the entity that triggered it
	Metadata     map[string]string
	Read         bool
	Dismissed    bool
	DeliveredVia []string // opaque channel names this was delivered on
	CreatedAt    time.Time
	ReadAt       *time.Time
	DismissedAt  *time.Time
	// IdempotencyKey, when set, makes the notification's creation idempotent per (Partition, Subject,
	// key): CreateNotification of a key already used answers the stored notification with
	// ErrNotificationExists and stores nothing, so Notify, retried with the same key, delivers once.
	// It is write-side only: no read returns it, except the stored row a replay answers.
	IdempotencyKey string
}

// DeviceToken is a registered push endpoint for one of a subject's devices. Platform and Provider are
// opaque strings (e.g. "android"/"ios", an FCM/APNs provider); an invalidated token is retained with
// InvalidatedAt set so a delivery attempt can prune it.
type DeviceToken struct {
	ID            string
	Partition     string // opaque partition-scope
	Subject       string
	Token         string
	Platform      string
	Provider      string
	CreatedAt     time.Time
	LastSeenAt    *time.Time
	InvalidatedAt *time.Time
}

// Preference is a subject's per-category channel preference: Channels maps an opaque channel name to
// whether the subject wants that category delivered on it. Category "*" is the default applied when no
// exact-category preference exists.
type Preference struct {
	Partition string
	Subject   string
	Category  string
	Channels  map[string]bool
	UpdatedAt time.Time
}
