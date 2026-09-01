package notify

import (
	"context"
	"time"
)

// Sink delivers a notification on one opaque channel (email, push, sms, webhook, or any consumer
// channel). A consumer implements one per channel and registers it with the Notifier; the package
// itself pulls no transport dependency. Channel() names the opaque channel it serves.
type Sink interface {
	Channel() string
	Deliver(ctx context.Context, n *Notification) error
}

// EffectiveChannels resolves which opaque channels a category should deliver on, given a user's
// preferences. An exact-category preference wins; else the "*" default preference; else the supplied
// defaultChannels. Only channels whose preference is true are returned (a preference present but false
// suppresses that channel). This is the source's per-category resolution, generalised to opaque channels.
func EffectiveChannels(prefs []*Preference, category string, defaultChannels []string) []string {
	var exact, star *Preference
	for _, p := range prefs {
		switch p.Category {
		case category:
			exact = p
		case "*":
			star = p
		}
	}
	pick := exact
	if pick == nil {
		pick = star
	}
	if pick == nil {
		return defaultChannels
	}
	var out []string
	for ch, on := range pick.Channels {
		if on {
			out = append(out, ch)
		}
	}
	return out
}

// Notifier persists a notification and fans it out to the pluggable Sinks matching the user's effective
// channels. It always records the in-app notification (the store IS the in-app channel); other channels
// deliver only if a Sink is registered for them and the user's preferences admit them. The channels
// actually delivered (including "in_app") are recorded back on the notification via SetDeliveredVia.
type Notifier struct {
	store    Store
	sinks    map[string]Sink
	defaults []string
	now      func() time.Time
}

// NotifierConfig wires a Notifier. Store is required. DefaultChannels apply when a user has no
// preference for a category (an empty list means in-app only). Sinks are registered by their Channel().
type NotifierConfig struct {
	Store           Store
	Sinks           []Sink
	DefaultChannels []string
	Now             func() time.Time
}

// InAppChannel is the always-on channel the store itself satisfies; it needs no Sink.
const InAppChannel = "in_app"

// NewNotifier validates the config and returns the Notifier.
func NewNotifier(c NotifierConfig) (*Notifier, error) {
	if c.Store == nil {
		return nil, &configError{"notify: NotifierConfig.Store is required"}
	}
	n := &Notifier{store: c.Store, sinks: map[string]Sink{}, defaults: c.DefaultChannels, now: c.Now}
	for _, s := range c.Sinks {
		n.sinks[s.Channel()] = s
	}
	if n.now == nil {
		n.now = time.Now
	}
	return n, nil
}

// configError reports an invalid configuration.
type configError struct{ msg string }

func (e *configError) Error() string { return e.msg }

// Notify persists the notification (the in-app channel) and delivers it on every effective non-in-app
// channel that has a registered Sink. A Sink error does not fail the whole call or lose the in-app
// record; the channel is simply omitted from DeliveredVia and its error collected. The delivered
// channels (always including in_app) are recorded on the notification. Returns per-channel Sink errors.
func (n *Notifier) Notify(ctx context.Context, notif *Notification) (deliverErrs map[string]error, err error) {
	if err := n.store.CreateNotification(ctx, notif); err != nil {
		return nil, err
	}
	prefs, err := n.store.GetPreferences(ctx, notif.Partition, notif.UserID)
	if err != nil {
		return nil, err
	}
	channels := EffectiveChannels(prefs, notif.Category, n.defaults)

	delivered := []string{InAppChannel}
	deliverErrs = map[string]error{}
	for _, ch := range channels {
		if ch == InAppChannel {
			continue // the store already satisfies in-app
		}
		sink, ok := n.sinks[ch]
		if !ok {
			continue // no transport registered for this channel — skip silently
		}
		if derr := sink.Deliver(ctx, notif); derr != nil {
			deliverErrs[ch] = derr
			continue
		}
		delivered = append(delivered, ch)
	}
	notif.DeliveredVia = delivered
	if serr := n.store.SetDeliveredVia(ctx, notif.ID, delivered); serr != nil {
		return deliverErrs, serr
	}
	return deliverErrs, nil
}
