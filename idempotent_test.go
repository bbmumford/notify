package notify

import (
	"errors"
	"testing"
)

func countNotifications(t *testing.T, s Store, partition, subject string) int {
	t.Helper()
	page, _, err := s.ListNotifications(ctx, partition, subject, "", false, 100, "")
	if err != nil {
		t.Fatal(err)
	}
	return len(page)
}

// A retried Notify of the same notification (its ID set by the first call)
// stores one inbox row and delivers once.
func TestNotify_ARetryOfTheSameNotificationDeliversOnce(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		s.SetPreference(ctx, &Preference{Partition: "acme", Subject: "u1", Category: "*", Channels: map[string]bool{"in_app": true, "email": true}})
		email := &recordingSink{channel: "email"}
		nf, err := NewNotifier(NotifierConfig{Store: s, Sinks: []Sink{email}})
		if err != nil {
			t.Fatal(err)
		}
		n := &Notification{Partition: "acme", Subject: "u1", Category: "billing", Title: "Invoice", CreatedAt: at}
		if _, err := nf.Notify(ctx, n); err != nil {
			t.Fatalf("first notify: %v", err)
		}
		if _, err := nf.Notify(ctx, n); err != nil {
			t.Fatalf("retried notify: %v", err)
		}
		if len(email.delivered) != 1 {
			t.Fatalf("email delivered %d times, want once", len(email.delivered))
		}
		if got := countNotifications(t, s, "acme", "u1"); got != 1 {
			t.Fatalf("%d inbox rows, want 1", got)
		}
	})
}

// Two Notify calls carrying the same idempotency key for one subject store one
// row and deliver once; the second notification is answered with the first.
func TestNotify_TheSameIdempotencyKeyDeliversOnce(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		s.SetPreference(ctx, &Preference{Partition: "acme", Subject: "u1", Category: "*", Channels: map[string]bool{"in_app": true, "email": true}})
		email := &recordingSink{channel: "email"}
		nf, _ := NewNotifier(NotifierConfig{Store: s, Sinks: []Sink{email}})
		first := &Notification{Partition: "acme", Subject: "u1", Category: "billing", Title: "Invoice", IdempotencyKey: "inv-7", CreatedAt: at}
		second := &Notification{Partition: "acme", Subject: "u1", Category: "billing", Title: "Invoice", IdempotencyKey: "inv-7", CreatedAt: at}
		if _, err := nf.Notify(ctx, first); err != nil {
			t.Fatalf("first notify: %v", err)
		}
		if _, err := nf.Notify(ctx, second); err != nil {
			t.Fatalf("retried notify: %v", err)
		}
		if len(email.delivered) != 1 {
			t.Fatalf("email delivered %d times, want once", len(email.delivered))
		}
		if got := countNotifications(t, s, "acme", "u1"); got != 1 {
			t.Fatalf("%d inbox rows, want 1", got)
		}
		if second.ID != first.ID || second.IdempotencyKey != "inv-7" {
			t.Fatalf("the replay answered id %q key %q, want the first notification %q", second.ID, second.IdempotencyKey, first.ID)
		}
	})
}

// The key is scoped to a partition and subject: another subject's use of it
// is its own notification.
func TestCreateNotification_TheKeyIsPerPartitionAndSubject(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		for _, owner := range [][2]string{{"acme", "u1"}, {"acme", "u2"}, {"other", "u1"}} {
			n := &Notification{Partition: owner[0], Subject: owner[1], Title: "Hi", IdempotencyKey: "k", CreatedAt: at}
			if err := s.CreateNotification(ctx, n); err != nil {
				t.Fatalf("%v: %v", owner, err)
			}
		}
		again := &Notification{Partition: "acme", Subject: "u2", Title: "Hi", IdempotencyKey: "k", CreatedAt: at}
		if err := s.CreateNotification(ctx, again); !errors.Is(err, ErrNotificationExists) {
			t.Fatalf("reused key: got %v, want ErrNotificationExists", err)
		}
		for _, owner := range [][2]string{{"acme", "u1"}, {"acme", "u2"}, {"other", "u1"}} {
			if got := countNotifications(t, s, owner[0], owner[1]); got != 1 {
				t.Fatalf("%v holds %d rows, want 1", owner, got)
			}
		}
	})
}

// An ID stored under another partition is refused, and nothing of the stored
// notification is answered.
func TestCreateNotification_AnIDTakenElsewhereIsRefused(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		if err := s.CreateNotification(ctx, &Notification{ID: "n-1", Partition: "acme", Subject: "u1", Title: "secret", CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
		other := &Notification{ID: "n-1", Partition: "evil", Subject: "u9", Title: "mine", CreatedAt: at}
		if err := s.CreateNotification(ctx, other); !errors.Is(err, ErrNotificationIDTaken) {
			t.Fatalf("got %v, want ErrNotificationIDTaken", err)
		}
		if other.Title != "mine" {
			t.Fatalf("the refusal answered the stored notification's title %q", other.Title)
		}
		if got, err := s.GetNotification(ctx, "acme", "u1", "n-1"); err != nil || got.Title != "secret" {
			t.Fatalf("the stored notification changed: %+v %v", got, err)
		}
	})
}
