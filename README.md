# notify

A per-person notification system. It keeps each subject's in-app inbox (create, list, unread count, mark read, dismiss), their push device tokens and their per-category channel preferences, and fans a new notification out to whatever delivery channels (email, push, SMS, webhook, anything) you plug in and the subject's preferences allow. A user event published with a non-empty idempotency key is recorded once, so a retried publish does not notify twice. Partitions and subjects are opaque ids.

## Use

```sh
go get github.com/bbmumford/notify
```

```go
store := notify.NewSQLStore(db) // any database/sql driver; or notify.NewMemStore()
if err := store.InitSchema(ctx); err != nil { /* ... */ } // idempotent; once at boot

n, err := notify.NewNotifier(notify.NotifierConfig{
	Store:           store,
	Sinks:           []notify.Sink{emailSink, pushSink}, // each names its Channel() and Delivers
	DefaultChannels: []string{"in_app", "email"},
})

failed, err := n.Notify(ctx, &notify.Notification{ /* partition, subject, category, title ... */ })
```

`Notify` always stores the in-app record, then delivers on every other channel the subject's preferences (or the defaults) select that has a `Sink`. A failing sink does not fail the call or lose the in-app record: its channel is left out of the delivered list and its error comes back in `failed`.

## Consumers

ORBTR library (its notification host), io tools and the node.orbtr.io endpoint.
