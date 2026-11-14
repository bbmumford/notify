package notify

import (
	"context"
	"errors"
	"testing"
)

func TestUserEvent_RecordAndGet(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		got, err := s.RecordUserEvent(ctx, &UserEvent{
			Partition: "p1", Subject: "u1", EventType: "welcome", IdempotencyKey: "k1",
			Title: "Hi", Body: "b", Channels: []string{"in_app", "email"},
		})
		if err != nil {
			t.Fatalf("RecordUserEvent: %v", err)
		}
		if got.EventID == "" || got.CreatedAt == 0 {
			t.Fatalf("event id/createdAt not minted: %+v", got)
		}
		read, err := s.GetUserEventByIdempotency(ctx, "p1", "u1", "k1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if read.EventID != got.EventID || read.Title != "Hi" || len(read.Channels) != 2 {
			t.Fatalf("read mismatch: %+v", read)
		}
	})
}

// A repeat with the same non-empty key returns the prior row + the sentinel.
func TestUserEvent_IdempotentReplay(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		first, err := s.RecordUserEvent(ctx, &UserEvent{Partition: "p1", Subject: "u1", EventType: "e", IdempotencyKey: "dup"})
		if err != nil {
			t.Fatalf("first: %v", err)
		}
		again, err := s.RecordUserEvent(ctx, &UserEvent{Partition: "p1", Subject: "u1", EventType: "e", IdempotencyKey: "dup", Title: "different"})
		if !errors.Is(err, ErrUserEventAlreadyPublished) {
			t.Fatalf("want ErrUserEventAlreadyPublished, got %v", err)
		}
		if again.EventID != first.EventID {
			t.Fatalf("replay returned a different event id: %s vs %s", again.EventID, first.EventID)
		}
	})
}

// An empty idempotency key never dedups — two records are two rows.
func TestUserEvent_EmptyKeyNeverDedups(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		a, err := s.RecordUserEvent(ctx, &UserEvent{Partition: "p1", Subject: "u1", EventType: "e"})
		if err != nil {
			t.Fatalf("a: %v", err)
		}
		b, err := s.RecordUserEvent(ctx, &UserEvent{Partition: "p1", Subject: "u1", EventType: "e"})
		if err != nil {
			t.Fatalf("b: %v", err)
		}
		if a.EventID == b.EventID {
			t.Fatal("empty-key records collided into one row")
		}
	})
}

func TestUserEvent_UpdateOutcome(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		ev, err := s.RecordUserEvent(ctx, &UserEvent{Partition: "p1", Subject: "u1", EventType: "e", IdempotencyKey: "k"})
		if err != nil {
			t.Fatalf("record: %v", err)
		}
		if err := s.UpdateUserEventOutcome(ctx, ev.EventID, true, 3, []string{"email"}); err != nil {
			t.Fatalf("update: %v", err)
		}
		read, err := s.GetUserEventByIdempotency(ctx, "p1", "u1", "k")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !read.Delivered || read.SubscriberCount != 3 || len(read.Channels) != 1 || read.Channels[0] != "email" {
			t.Fatalf("outcome not recorded: %+v", read)
		}
		if err := s.UpdateUserEventOutcome(ctx, "nope", true, 0, nil); !errors.Is(err, ErrNotFound) {
			t.Fatalf("update unknown: want ErrNotFound, got %v", err)
		}
	})
}

func TestUserEvent_GetMissingIsNotFound(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		if _, err := s.GetUserEventByIdempotency(context.Background(), "p", "u", "nope"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	})
}

func TestUserEvent_Validation(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		ctx := context.Background()
		for _, e := range []*UserEvent{
			{Subject: "u", EventType: "e"},   // no partition
			{Partition: "p", EventType: "e"}, // no user
			{Partition: "p", Subject: "u"},   // no event type
		} {
			if _, err := s.RecordUserEvent(ctx, e); err == nil {
				t.Fatalf("incomplete event should fail: %+v", e)
			}
		}
	})
}
