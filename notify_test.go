package notify

import (
	"context"
	"errors"
	"testing"
	"time"
)

var ctx = context.Background()

func storeAt(t time.Time) *MemStore {
	m := NewMemStore()
	m.now = func() time.Time { return t }
	return m
}

func TestNotifications_LifecycleAndOwnerScope(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	m := storeAt(now)
	n := &Notification{Owner: "acme", UserID: "u1", Category: "billing", Title: "Invoice", Body: "due"}
	if err := m.CreateNotification(ctx, n); err != nil || n.ID == "" {
		t.Fatalf("create: %v", err)
	}
	// owner + user scoping on get
	if _, err := m.GetNotification(ctx, "globex", "u1", n.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("a different owner must not see the notification")
	}
	if _, err := m.GetNotification(ctx, "acme", "u2", n.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("a different user must not see the notification")
	}
	// unread count
	if c, latest, _ := m.UnreadCount(ctx, "acme", "u1"); c != 1 || latest == nil {
		t.Fatalf("unread count: %d latest=%v", c, latest)
	}
	// mark read
	if got, _ := m.MarkRead(ctx, "acme", "u1", []string{n.ID}); got != 1 {
		t.Fatalf("MarkRead affected %d, want 1", got)
	}
	if c, _, _ := m.UnreadCount(ctx, "acme", "u1"); c != 0 {
		t.Fatalf("after read, unread should be 0; got %d", c)
	}
	// a read notification stamps ReadAt
	got, _ := m.GetNotification(ctx, "acme", "u1", n.ID)
	if !got.Read || got.ReadAt == nil {
		t.Fatal("read must stamp ReadAt")
	}
	// dismiss removes it from listings
	if err := m.Dismiss(ctx, "acme", "u1", n.ID); err != nil {
		t.Fatal(err)
	}
	if page, _, _ := m.ListNotifications(ctx, "acme", "u1", "", false, 0, ""); len(page) != 0 {
		t.Fatalf("a dismissed notification must not list; got %d", len(page))
	}
}

func TestNotifications_ListFilterPagingSince(t *testing.T) {
	m := NewMemStore()
	base := time.Unix(2000, 0).UTC()
	for i, cat := range []string{"a", "b", "a", "b", "a"} {
		m.now = func() time.Time { return base.Add(time.Duration(i) * time.Second) }
		n := &Notification{Owner: "acme", UserID: "u1", Category: cat, Title: cat}
		m.CreateNotification(ctx, n)
	}
	// category filter
	if page, _, _ := m.ListNotifications(ctx, "acme", "u1", "a", false, 0, ""); len(page) != 3 {
		t.Fatalf("category 'a' filter: got %d want 3", len(page))
	}
	// paging via opaque cursor
	p1, next, _ := m.ListNotifications(ctx, "acme", "u1", "", false, 2, "")
	if len(p1) != 2 || next == "" {
		t.Fatalf("page 1: len=%d next=%q", len(p1), next)
	}
	p2, next2, _ := m.ListNotifications(ctx, "acme", "u1", "", false, 2, next)
	if len(p2) != 2 || next2 == "" {
		t.Fatalf("page 2: len=%d next=%q", len(p2), next2)
	}
	p3, next3, _ := m.ListNotifications(ctx, "acme", "u1", "", false, 2, next2)
	if len(p3) != 1 || next3 != "" {
		t.Fatalf("page 3 (last): len=%d next=%q", len(p3), next3)
	}
	// since (strictly after)
	since, _ := m.ListNotificationsSince(ctx, "acme", "u1", base.Add(2*time.Second))
	if len(since) != 2 {
		t.Fatalf("since: got %d want 2 (created at +3,+4)", len(since))
	}
}

func TestDeviceTokens_RegisterActiveInvalidateCleanup(t *testing.T) {
	now := time.Unix(3000, 0).UTC()
	m := storeAt(now)
	m.RegisterToken(ctx, &DeviceToken{Owner: "acme", UserID: "u1", Token: "tok-1", Platform: "ios"})
	// re-register the same token refreshes rather than duplicating
	m.RegisterToken(ctx, &DeviceToken{Owner: "acme", UserID: "u1", Token: "tok-1", Platform: "ios"})
	if toks, _ := m.ListActiveTokens(ctx, "acme", "u1"); len(toks) != 1 {
		t.Fatalf("re-register must not duplicate; got %d", len(toks))
	}
	// invalidate hides it from active
	m.InvalidateToken(ctx, "acme", "tok-1", now.UnixNano())
	if toks, _ := m.ListActiveTokens(ctx, "acme", "u1"); len(toks) != 0 {
		t.Fatalf("invalidated token must not be active; got %d", len(toks))
	}
	// a different owner cannot invalidate
	m.RegisterToken(ctx, &DeviceToken{Owner: "acme", UserID: "u1", Token: "tok-2", Platform: "ios"})
	if err := m.InvalidateToken(ctx, "globex", "tok-2", now.UnixNano()); !errors.Is(err, ErrNotFound) {
		t.Fatal("a different owner must not invalidate the token")
	}
	// cleanup removes stale-by-time
	removed, _ := m.CleanupStaleTokens(ctx, now.Add(time.Hour).UnixNano())
	if removed == 0 {
		t.Fatal("cleanup should remove tokens older than the cutoff")
	}
}

func TestPreferences_SetGetAndEffectiveChannels(t *testing.T) {
	m := NewMemStore()
	m.SetPreference(ctx, &Preference{Owner: "acme", UserID: "u1", Category: "billing", Channels: map[string]bool{"in_app": true, "email": true, "push": false}})
	m.SetPreference(ctx, &Preference{Owner: "acme", UserID: "u1", Category: "*", Channels: map[string]bool{"in_app": true}})
	// overwrite is idempotent (no duplicate rows)
	m.SetPreference(ctx, &Preference{Owner: "acme", UserID: "u1", Category: "billing", Channels: map[string]bool{"in_app": true, "email": true}})
	prefs, _ := m.GetPreferences(ctx, "acme", "u1")
	if len(prefs) != 2 {
		t.Fatalf("expected 2 preference rows (billing, *); got %d", len(prefs))
	}
	// exact-category preference wins and suppresses false channels
	eff := EffectiveChannels(prefs, "billing", []string{"in_app", "email", "push"})
	if !contains(eff, "in_app") || !contains(eff, "email") || contains(eff, "push") {
		t.Fatalf("effective channels for billing: %v (want in_app+email, not push)", eff)
	}
	// an unknown category falls to the '*' default
	if eff := EffectiveChannels(prefs, "unknown", []string{"in_app", "email"}); len(eff) != 1 || eff[0] != "in_app" {
		t.Fatalf("unknown category must use '*' default; got %v", eff)
	}
	// no preferences at all → provided defaults
	if eff := EffectiveChannels(nil, "x", []string{"in_app", "push"}); len(eff) != 2 {
		t.Fatalf("no prefs must return defaults; got %v", eff)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// recordingSink captures deliveries and can fail on demand — the pluggable transport under test.
type recordingSink struct {
	channel   string
	delivered []string
	failWith  error
}

func (r *recordingSink) Channel() string { return r.channel }
func (r *recordingSink) Deliver(_ context.Context, n *Notification) error {
	if r.failWith != nil {
		return r.failWith
	}
	r.delivered = append(r.delivered, n.ID)
	return nil
}

func TestNotifier_FanOutRespectsPreferencesAndSinks(t *testing.T) {
	m := NewMemStore()
	m.SetPreference(ctx, &Preference{Owner: "acme", UserID: "u1", Category: "billing", Channels: map[string]bool{"in_app": true, "email": true}})
	email := &recordingSink{channel: "email"}
	push := &recordingSink{channel: "push"}
	nf, err := NewNotifier(NotifierConfig{Store: m, Sinks: []Sink{email, push}})
	if err != nil {
		t.Fatal(err)
	}
	n := &Notification{Owner: "acme", UserID: "u1", Category: "billing", Title: "Invoice"}
	errs, err := nf.Notify(ctx, n)
	if err != nil || len(errs) != 0 {
		t.Fatalf("notify: err=%v deliverErrs=%v", err, errs)
	}
	// in-app is always recorded; email delivered (preference on + sink present); push not (preference absent)
	if !contains(n.DeliveredVia, "in_app") || !contains(n.DeliveredVia, "email") || contains(n.DeliveredVia, "push") {
		t.Fatalf("delivered via %v; want in_app+email only", n.DeliveredVia)
	}
	if len(email.delivered) != 1 || len(push.delivered) != 0 {
		t.Fatalf("email should get 1, push 0; got email=%d push=%d", len(email.delivered), len(push.delivered))
	}
	// the in-app record persisted and is retrievable
	if got, _ := m.GetNotification(ctx, "acme", "u1", n.ID); got == nil || !contains(got.DeliveredVia, "email") {
		t.Fatal("persisted notification must record the delivered channels")
	}
}

func TestNotifier_SinkFailureDoesNotLoseInApp(t *testing.T) {
	m := NewMemStore()
	m.SetPreference(ctx, &Preference{Owner: "acme", UserID: "u1", Category: "*", Channels: map[string]bool{"in_app": true, "email": true}})
	email := &recordingSink{channel: "email", failWith: errors.New("smtp down")}
	nf, _ := NewNotifier(NotifierConfig{Store: m, Sinks: []Sink{email}})
	n := &Notification{Owner: "acme", UserID: "u1", Category: "alerts", Title: "Alert"}
	errs, err := nf.Notify(ctx, n)
	if err != nil {
		t.Fatalf("a sink failure must not fail the whole Notify: %v", err)
	}
	if errs["email"] == nil {
		t.Fatal("the email sink error must be reported")
	}
	// in-app still recorded and persisted; email NOT in DeliveredVia
	if !contains(n.DeliveredVia, "in_app") || contains(n.DeliveredVia, "email") {
		t.Fatalf("delivered via %v; want in_app only (email failed)", n.DeliveredVia)
	}
	if got, _ := m.GetNotification(ctx, "acme", "u1", n.ID); got == nil {
		t.Fatal("the in-app notification must persist despite the sink failure")
	}
}
