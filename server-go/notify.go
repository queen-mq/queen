package main

import "sync"

// Notifier is the Go analogue of libqueen's pop backoff-tracker wake path
// (update_pop_backoff_tracker): a parked long-poll pop registers interest in a
// queue key and is woken the instant a push commits to that queue — instead of
// blindly re-polling. Broadcast is done by closing (and replacing) a per-key
// channel, so all current waiters wake at once.
//
// Lost-wakeup safety: a waiter MUST call Register(key) to obtain its wait
// channel BEFORE performing its pop attempt. If a push closes the channel
// between the attempt and the select, the waiter observes it already closed and
// re-attempts immediately.
type Notifier struct {
	mu    sync.Mutex
	chans map[string]chan struct{}
}

func NewNotifier() *Notifier {
	return &Notifier{chans: make(map[string]chan struct{})}
}

// Register returns the current wait channel for key, creating it if needed.
func (n *Notifier) Register(key string) <-chan struct{} {
	n.mu.Lock()
	ch, ok := n.chans[key]
	if !ok {
		ch = make(chan struct{})
		n.chans[key] = ch
	}
	n.mu.Unlock()
	return ch
}

// Notify wakes all current waiters on key (closes + drops the channel; the next
// Register creates a fresh one). Cheap no-op when nobody is parked on key.
func (n *Notifier) Notify(key string) {
	n.mu.Lock()
	if ch, ok := n.chans[key]; ok {
		close(ch)
		delete(n.chans, key)
	}
	n.mu.Unlock()
}
