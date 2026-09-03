package notify

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strconv"
	"sync"
	"time"
)

// MemStore is the in-memory reference Store — the test/dev backend and the semantic reference a durable
// (*sql.DB) store must match. Thread-safe; clock and id generator injectable. Partition is opaque; "" on an
// partition-scoped read means any partition.
type MemStore struct {
	mu     sync.Mutex
	notifs map[string]*Notification
	tokens map[string]*DeviceToken
	prefs  []*Preference
	events map[string]*UserEvent // user-event idempotency anchors, keyed by EventID
	now    func() time.Time
	newID  func() string
}

// NewMemStore builds an empty notify store.
func NewMemStore() *MemStore {
	return &MemStore{
		notifs: map[string]*Notification{},
		tokens: map[string]*DeviceToken{},
		events: map[string]*UserEvent{},
		now:    time.Now,
		newID:  randomID,
	}
}

func randomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func partitionMatch(want, have string) bool { return want == "" || want == have }

// ── Notifications ──

func (m *MemStore) CreateNotification(_ context.Context, n *Notification) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n.ID == "" {
		n.ID = m.newID()
	}
	if n.CreatedAt.IsZero() {
		n.CreatedAt = m.now()
	}
	m.notifs[n.ID] = cloneNotification(n)
	return nil
}

func (m *MemStore) GetNotification(_ context.Context, partition, userID, id string) (*Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.notifs[id]
	if !ok || !partitionMatch(partition, n.Partition) || (userID != "" && n.UserID != userID) {
		return nil, ErrNotFound
	}
	return cloneNotification(n), nil
}

func (m *MemStore) userNotifs(partition, userID string) []*Notification {
	var out []*Notification
	for _, n := range m.notifs {
		if partitionMatch(partition, n.Partition) && n.UserID == userID {
			out = append(out, n)
		}
	}
	// newest first, tie-broken by id for determinism
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID > out[j].ID
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out
}

func (m *MemStore) ListNotifications(_ context.Context, partition, userID, category string, unreadOnly bool, limit int, cursor string) ([]*Notification, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	all := m.userNotifs(partition, userID)
	var filtered []*Notification
	for _, n := range all {
		if n.Dismissed {
			continue
		}
		if unreadOnly && n.Read {
			continue
		}
		if category != "" && n.Category != category {
			continue
		}
		filtered = append(filtered, n)
	}
	// cursor is the 0-based offset into the filtered list (opaque to callers).
	start := 0
	if cursor != "" {
		if v, err := strconv.Atoi(cursor); err == nil && v > 0 {
			start = v
		}
	}
	if start >= len(filtered) {
		return nil, "", nil
	}
	page := filtered[start:]
	next := ""
	if limit > 0 && len(page) > limit {
		page = page[:limit]
		next = strconv.Itoa(start + limit)
	}
	out := make([]*Notification, len(page))
	for i, n := range page {
		out[i] = cloneNotification(n)
	}
	return out, next, nil
}

func (m *MemStore) ListNotificationsSince(_ context.Context, partition, userID string, since time.Time) ([]*Notification, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Notification
	for _, n := range m.userNotifs(partition, userID) {
		if n.Dismissed || !n.CreatedAt.After(since) {
			continue
		}
		out = append(out, cloneNotification(n))
	}
	return out, nil
}

func (m *MemStore) UnreadCount(_ context.Context, partition, userID string) (int, *time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	var latest *time.Time
	for _, n := range m.userNotifs(partition, userID) {
		if n.Read || n.Dismissed {
			continue
		}
		count++
		if latest == nil || n.CreatedAt.After(*latest) {
			c := n.CreatedAt
			latest = &c
		}
	}
	return count, latest, nil
}

func (m *MemStore) MarkRead(_ context.Context, partition, userID string, ids []string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	n := 0
	for _, notif := range m.notifs {
		if want[notif.ID] && partitionMatch(partition, notif.Partition) && notif.UserID == userID && !notif.Read {
			notif.Read = true
			t := m.now()
			notif.ReadAt = &t
			n++
		}
	}
	return n, nil
}

func (m *MemStore) MarkAllRead(_ context.Context, partition, userID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, notif := range m.notifs {
		if partitionMatch(partition, notif.Partition) && notif.UserID == userID && !notif.Read && !notif.Dismissed {
			notif.Read = true
			t := m.now()
			notif.ReadAt = &t
			n++
		}
	}
	return n, nil
}

func (m *MemStore) Dismiss(_ context.Context, partition, userID, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.notifs[id]
	if !ok || !partitionMatch(partition, n.Partition) || n.UserID != userID {
		return ErrNotFound
	}
	if !n.Dismissed {
		n.Dismissed = true
		t := m.now()
		n.DismissedAt = &t
	}
	return nil
}

func (m *MemStore) SetDeliveredVia(_ context.Context, id string, channels []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.notifs[id]
	if !ok {
		return ErrNotFound
	}
	n.DeliveredVia = append([]string(nil), channels...)
	return nil
}

// ── Push device tokens ──

func (m *MemStore) RegisterToken(_ context.Context, t *DeviceToken) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// A token string is unique; re-registering refreshes LastSeenAt and clears invalidation.
	for _, e := range m.tokens {
		if e.Token == t.Token {
			now := m.now()
			e.LastSeenAt = &now
			e.InvalidatedAt = nil
			e.UserID = t.UserID
			e.Partition = t.Partition
			e.Platform = t.Platform
			e.Provider = t.Provider
			return nil
		}
	}
	if t.ID == "" {
		t.ID = m.newID()
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = m.now()
	}
	m.tokens[t.ID] = cloneToken(t)
	return nil
}

func (m *MemStore) ListActiveTokens(_ context.Context, partition, userID string) ([]*DeviceToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*DeviceToken
	for _, t := range m.tokens {
		if partitionMatch(partition, t.Partition) && t.UserID == userID && t.InvalidatedAt == nil {
			out = append(out, cloneToken(t))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (m *MemStore) InvalidateToken(_ context.Context, partition, token string, at int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.tokens {
		if t.Token == token && partitionMatch(partition, t.Partition) {
			if t.InvalidatedAt == nil {
				ts := time.Unix(0, at).UTC()
				t.InvalidatedAt = &ts
			}
			return nil
		}
	}
	return ErrNotFound
}

func (m *MemStore) CleanupStaleTokens(_ context.Context, before int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := time.Unix(0, before).UTC()
	n := 0
	for id, t := range m.tokens {
		last := t.CreatedAt
		if t.LastSeenAt != nil {
			last = *t.LastSeenAt
		}
		if last.Before(cutoff) {
			delete(m.tokens, id)
			n++
		}
	}
	return n, nil
}

// ── Preferences ──

func (m *MemStore) GetPreferences(_ context.Context, partition, userID string) ([]*Preference, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Preference
	for _, p := range m.prefs {
		if partitionMatch(partition, p.Partition) && p.UserID == userID {
			out = append(out, clonePreference(p))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Category < out[j].Category })
	return out, nil
}

func (m *MemStore) SetPreference(_ context.Context, p *Preference) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.prefs {
		if e.Partition == p.Partition && e.UserID == p.UserID && e.Category == p.Category {
			e.Channels = cloneBoolMap(p.Channels)
			e.UpdatedAt = m.now()
			return nil
		}
	}
	cp := clonePreference(p)
	cp.UpdatedAt = m.now()
	m.prefs = append(m.prefs, cp)
	return nil
}

// ── clone helpers ──

func clonePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

func cloneStrMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	c := make(map[string]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func cloneBoolMap(m map[string]bool) map[string]bool {
	if m == nil {
		return nil
	}
	c := make(map[string]bool, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

func cloneNotification(n *Notification) *Notification {
	cp := *n
	cp.Metadata = cloneStrMap(n.Metadata)
	if n.DeliveredVia != nil {
		cp.DeliveredVia = append([]string(nil), n.DeliveredVia...)
	}
	cp.ReadAt = clonePtr(n.ReadAt)
	cp.DismissedAt = clonePtr(n.DismissedAt)
	return &cp
}

func cloneToken(t *DeviceToken) *DeviceToken {
	cp := *t
	cp.LastSeenAt = clonePtr(t.LastSeenAt)
	cp.InvalidatedAt = clonePtr(t.InvalidatedAt)
	return &cp
}

func clonePreference(p *Preference) *Preference {
	cp := *p
	cp.Channels = cloneBoolMap(p.Channels)
	return &cp
}

var _ Store = (*MemStore)(nil)
