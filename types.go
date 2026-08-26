// Package notify is the scope-agnostic notification system (extracted from the HSTLES notify domain). It
// owns in-app notifications, per-user push device tokens, and per-category channel preferences over an
// injected store, and fans a notification out to pluggable delivery Sinks. The primary axis (a baked
// tenant) is an opaque Owner the system never interprets; the user id is opaque; and delivery channels
// are opaque strings a consumer registers a Sink for ("in_app", "email", "push", "sms", "webhook", or
// anything else) — so the channel set is extensible, not a fixed enum. Times are time.Time; a nil
// *time.Time means "not yet" (unread / not dismissed / not invalidated).
package notify

import "time"

// Notification is a single notification addressed to a user. Owner is the opaque owner-scope; Metadata
// is opaque JSON a consumer attaches; DeliveredVia records the opaque channel names it was delivered on.
type Notification struct {
	ID           string
	Owner        string // opaque owner-scope (was tenant)
	UserID       string
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
}

// DeviceToken is a registered push endpoint for one of a user's devices. Platform and Provider are
// opaque strings (e.g. "android"/"ios", an FCM/APNs provider); an invalidated token is retained with
// InvalidatedAt set so a delivery attempt can prune it.
type DeviceToken struct {
	ID            string
	Owner         string // opaque owner-scope
	UserID        string
	Token         string
	Platform      string
	Provider      string
	CreatedAt     time.Time
	LastSeenAt    *time.Time
	InvalidatedAt *time.Time
}

// Preference is a user's per-category channel preference: Channels maps an opaque channel name to
// whether the user wants that category delivered on it. Category "*" is the default applied when no
// exact-category preference exists.
type Preference struct {
	Owner     string
	UserID    string
	Category  string
	Channels  map[string]bool
	UpdatedAt time.Time
}
