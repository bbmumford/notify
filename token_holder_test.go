package notify

import (
	"errors"
	"testing"
)

func tokenHolders(t *testing.T, s Store, partition, subject string) []string {
	t.Helper()
	toks, err := s.ListActiveTokens(ctx, partition, subject)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, tk := range toks {
		out = append(out, tk.Token)
	}
	return out
}

// A valid push token stays with the subject that registered it: another
// subject presenting the same token is refused and the holder keeps it.
func TestRegisterToken_AValidTokenIsNotMovedToAnotherSubject(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		if err := s.RegisterToken(ctx, &DeviceToken{Partition: "acme", Subject: "u1", Token: "tok-1", Platform: "ios", CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
		err := s.RegisterToken(ctx, &DeviceToken{Partition: "evil", Subject: "u9", Token: "tok-1", Platform: "ios", CreatedAt: at})
		if err == nil {
			t.Fatal("another subject's registration of a held token was accepted")
		}
		if got := tokenHolders(t, s, "acme", "u1"); len(got) != 1 {
			t.Fatalf("the holder's tokens are %v, want tok-1 kept", got)
		}
		if got := tokenHolders(t, s, "evil", "u9"); len(got) != 0 {
			t.Fatalf("the other subject now holds %v", got)
		}
	})
}

// The holder re-registers its own token freely, and an invalidated token may
// be registered by the device's next subject.
func TestRegisterToken_TheHolderAndAnInvalidatedTokenReRegister(t *testing.T) {
	eachStore(t, func(t *testing.T, s Store) {
		s.RegisterToken(ctx, &DeviceToken{Partition: "acme", Subject: "u1", Token: "tok-1", Platform: "ios", CreatedAt: at})
		if err := s.RegisterToken(ctx, &DeviceToken{Partition: "acme", Subject: "u1", Token: "tok-1", Platform: "android", CreatedAt: at}); err != nil {
			t.Fatalf("the holder's re-register: %v", err)
		}
		if err := s.RegisterToken(ctx, &DeviceToken{Partition: "acme", Subject: "u2", Token: "tok-1", Platform: "ios", CreatedAt: at}); !errors.Is(err, ErrTokenHeld) {
			t.Fatalf("another subject of the same partition: got %v, want ErrTokenHeld", err)
		}
		if err := s.InvalidateToken(ctx, "acme", "tok-1", at.Unix()); err != nil {
			t.Fatal(err)
		}
		if err := s.RegisterToken(ctx, &DeviceToken{Partition: "acme", Subject: "u2", Token: "tok-1", Platform: "ios", CreatedAt: at}); err != nil {
			t.Fatalf("an invalidated token's registration by the next subject: %v", err)
		}
		if got := tokenHolders(t, s, "acme", "u2"); len(got) != 1 {
			t.Fatalf("the next subject holds %v, want tok-1", got)
		}
		if got := tokenHolders(t, s, "acme", "u1"); len(got) != 0 {
			t.Fatalf("the earlier subject still holds %v", got)
		}
	})
}
