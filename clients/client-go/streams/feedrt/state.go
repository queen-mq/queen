package feedrt

import (
	"strings"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
)

// FeedState is the state of one stateful step: its open windows, sessions,
// waiting messages or table rows, by state key, in memory (state.js).
//
// It iterates in the order keys were first set, as a JS Map does (a key set
// again keeps its place, one deleted and set again goes last), so a live
// list and a snapshot read the same way in every SDK. Every change is
// recorded so it can be undone: a function that fails leaves no trace, and a
// commit that did not happen rolls back to the last one that did.
type FeedState struct {
	m          map[string]*stateNode
	head, tail *stateNode
	undo       []undoEntry
	// Anything set or deleted since the last snapshot: the next one has work.
	changed bool
	// The keys set or deleted since the last snapshot: what a delta writes.
	touched orderedSet
	// How many entries of each class the state holds, kept as it changes.
	classify func(key string, value interface{}) string
	counts   map[string]int
}

type stateNode struct {
	key        string
	val        interface{}
	prev, next *stateNode
}

type undoEntry struct {
	key  string
	had  bool
	prev interface{}
}

// KV is one entry: a key and its value (Undefined in a delta: the key is gone).
type KV struct {
	K string
	V interface{}
}

// NewFeedState is an empty state.
func NewFeedState() *FeedState {
	return &FeedState{m: map[string]*stateNode{}, counts: map[string]int{}}
}

// Size is how many keys it holds.
func (s *FeedState) Size() int { return len(s.m) }

// Dirty: something changed since the last commit.
func (s *FeedState) Dirty() bool { return len(s.undo) > 0 }

// ChangedSinceSnapshot: something changed since the last snapshot.
func (s *FeedState) ChangedSinceSnapshot() bool { return s.changed }

// TouchedSinceSnapshot is how many keys were set or deleted since the last snapshot.
func (s *FeedState) TouchedSinceSnapshot() int { return s.touched.len() }

// SnapshotTaken: the state as it is now is in a snapshot.
func (s *FeedState) SnapshotTaken() {
	s.changed = false
	s.touched.clear()
}

// ClassifyWith counts the entries by class: fn answers a class name or "".
func (s *FeedState) ClassifyWith(fn func(key string, value interface{}) string) {
	s.classify = fn
	s.recount()
}

// Count is how many entries of class name the state holds.
func (s *FeedState) Count(name string) int { return s.counts[name] }

// Get answers the value of key (ok false when absent).
func (s *FeedState) Get(key string) (interface{}, bool) {
	if n, ok := s.m[key]; ok {
		return n.val, true
	}
	return nil, false
}

// Value answers the value of key, or nil.
func (s *FeedState) Value(key string) interface{} {
	v, _ := s.Get(key)
	return v
}

// Has: the key is there.
func (s *FeedState) Has(key string) bool {
	_, ok := s.m[key]
	return ok
}

// Each calls fn for every entry in order; fn answers false to stop.
func (s *FeedState) Each(fn func(key string, value interface{}) bool) {
	for n := s.head; n != nil; {
		next := n.next
		if !fn(n.key, n.val) {
			return
		}
		n = next
	}
}

func (s *FeedState) put(key string, value interface{}) {
	if n, ok := s.m[key]; ok {
		n.val = value
		return
	}
	n := &stateNode{key: key, val: value, prev: s.tail}
	if s.tail != nil {
		s.tail.next = n
	} else {
		s.head = n
	}
	s.tail = n
	s.m[key] = n
}

func (s *FeedState) remove(key string) {
	n, ok := s.m[key]
	if !ok {
		return
	}
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		s.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		s.tail = n.prev
	}
	delete(s.m, key)
}

func (s *FeedState) reset() {
	s.m = map[string]*stateNode{}
	s.head, s.tail = nil, nil
}

// Set stores value (a fresh value: stored values are never changed in place).
func (s *FeedState) Set(key string, value interface{}) {
	prev, had := s.Get(key)
	s.undo = append(s.undo, undoEntry{key: key, had: had, prev: prev})
	if had {
		s.bump(key, prev, -1)
	}
	s.put(key, value)
	s.bump(key, value, 1)
	s.changed = true
	s.touched.add(key)
}

// Delete removes key.
func (s *FeedState) Delete(key string) {
	prev, had := s.Get(key)
	if !had {
		return
	}
	s.undo = append(s.undo, undoEntry{key: key, had: true, prev: prev})
	s.bump(key, prev, -1)
	s.remove(key)
	s.changed = true
	s.touched.add(key)
}

// Mark is a point to roll back to.
func (s *FeedState) Mark() int { return len(s.undo) }

// RollbackTo undoes every change after mark.
func (s *FeedState) RollbackTo(mark int) {
	for len(s.undo) > mark {
		u := s.undo[len(s.undo)-1]
		s.undo = s.undo[:len(s.undo)-1]
		if cur, ok := s.Get(u.key); ok {
			s.bump(u.key, cur, -1)
		}
		if u.had {
			s.put(u.key, u.prev)
			s.bump(u.key, u.prev, 1)
		} else {
			s.remove(u.key)
		}
		// The key stays touched: a delta writes it as it is now.
	}
}

// Discard goes back to the state of the last commit.
func (s *FeedState) Discard() { s.RollbackTo(0) }

// Committed: the changes are the new starting point.
func (s *FeedState) Committed() { s.undo = nil }

// Dump is every entry, for a snapshot.
func (s *FeedState) Dump() []KV {
	out := make([]KV, 0, len(s.m))
	s.Each(func(k string, v interface{}) bool {
		out = append(out, KV{k, v})
		return true
	})
	return out
}

// Touched is what a delta snapshot writes: each key touched since the last
// snapshot, with its value, or Undefined when it is gone.
func (s *FeedState) Touched() []KV {
	out := make([]KV, 0, s.touched.len())
	s.touched.each(func(k string) {
		if v, ok := s.Get(k); ok {
			out = append(out, KV{k, v})
		} else {
			out = append(out, KV{k, undefinedValue})
		}
	})
	return out
}

// Load replaces everything with a snapshot's entries; nothing to undo.
func (s *FeedState) Load(entries []KV) {
	s.reset()
	for _, e := range entries {
		s.put(e.K, e.V)
	}
	s.undo = nil
	s.changed = false
	s.touched.clear()
	s.recount()
}

// LoadBegin starts a load in pieces: empty, then LoadSet / LoadDelete each piece, then Loaded.
func (s *FeedState) LoadBegin() {
	s.reset()
	s.undo = nil
	s.counts = map[string]int{}
}

// LoadSet is one entry of a piece.
func (s *FeedState) LoadSet(key string, value interface{}) {
	if cur, ok := s.Get(key); ok {
		s.bump(key, cur, -1)
	}
	s.put(key, value)
	s.bump(key, value, 1)
}

// LoadDelete is one delete of a piece.
func (s *FeedState) LoadDelete(key string) {
	cur, ok := s.Get(key)
	if !ok {
		return
	}
	s.bump(key, cur, -1)
	s.remove(key)
}

// Loaded: the pieces are in; nothing to undo or to write.
func (s *FeedState) Loaded() {
	s.undo = nil
	s.changed = false
	s.touched.clear()
}

// Wipe starts empty (a reset): the next snapshot writes the empty state.
func (s *FeedState) Wipe() {
	s.reset()
	s.undo = nil
	s.changed = true
	s.touched.clear()
	s.counts = map[string]int{}
}

func (s *FeedState) bump(key string, value interface{}, d int) {
	if s.classify == nil {
		return
	}
	if c := s.classify(key, value); c != "" {
		s.counts[c] += d
	}
}

func (s *FeedState) recount() {
	s.counts = map[string]int{}
	if s.classify == nil {
		return
	}
	s.Each(func(k string, v interface{}) bool {
		s.bump(k, v, 1)
		return true
	})
}

// orderedSet is a JS Set of strings: insertion order, an added key keeps its place.
type orderedSet struct {
	idx  map[string]int
	keys []string
}

func (o *orderedSet) add(k string) {
	if o.idx == nil {
		o.idx = map[string]int{}
	}
	if _, ok := o.idx[k]; ok {
		return
	}
	o.idx[k] = len(o.keys)
	o.keys = append(o.keys, k)
}

func (o *orderedSet) len() int { return len(o.keys) }

func (o *orderedSet) clear() {
	o.idx = nil
	o.keys = nil
}

func (o *orderedSet) each(fn func(string)) {
	for _, k := range o.keys {
		fn(k)
	}
}

// BufferPrefix is where an event-time step keeps the messages it holds until
// its watermark reaches them (clock.go); the engines leave them alone.
const BufferPrefix = "b\u001f"

// RebuildBatch is how many entries are indexed between two yields when a loaded state is indexed again.
const RebuildBatch = 50_000

// CloseBatch is how many windows, deadlines or runs close in one go; then the
// results run, then the next batch.
const CloseBatch = 2_000

func isBufferKey(key string) bool { return strings.HasPrefix(key, BufferPrefix) }

type dueItem struct {
	due float64
	key string
}

// before: the earlier due, then the smaller key (ties close in one order every time).
func before(x, y dueItem) bool {
	return x.due < y.due || (x.due == y.due && x.key < y.key)
}

// DueHeap is a min-heap of (due, key): the next window or deadline to close.
type DueHeap struct {
	a []dueItem
}

func (h *DueHeap) size() int { return len(h.a) }

func (h *DueHeap) clear() { h.a = nil }

func (h *DueHeap) peek() dueItem { return h.a[0] }

func (h *DueHeap) push(due float64, key string) {
	a := append(h.a, dueItem{due, key})
	i := len(a) - 1
	for i > 0 {
		parent := (i - 1) >> 1
		if !before(a[i], a[parent]) {
			break
		}
		a[parent], a[i] = a[i], a[parent]
		i = parent
	}
	h.a = a
}

func (h *DueHeap) pop() dueItem {
	a := h.a
	top := a[0]
	last := a[len(a)-1]
	a = a[:len(a)-1]
	if len(a) > 0 {
		a[0] = last
		i := 0
		for {
			l := 2*i + 1
			r := l + 1
			m := i
			if l < len(a) && before(a[l], a[m]) {
				m = l
			}
			if r < len(a) && before(a[r], a[m]) {
				m = r
			}
			if m == i {
				break
			}
			a[m], a[i] = a[i], a[m]
			i = m
		}
	}
	h.a = a
	return top
}

// FNV1a32 is the broker's shard hash (feed.rs): FNV-1a, 32 bit, over UTF-8.
func FNV1a32(text string) uint32 { return feed.FNV1a32(text) }

// ShardOf is the shard of count a key belongs to.
func ShardOf(key string, count int) int { return feed.ShardOf(key, count) }
