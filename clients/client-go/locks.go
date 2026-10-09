package queen

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	mrand "math/rand"
	"os"
	"sync"
	"time"
)

// Locks: a lock and a semaphore, as leases with a fencing token.
//
//	lock := client.Lock("daily-report", 30*time.Second)
//	ok, err := lock.TryAcquire(ctx)
//	if err != nil || !ok {
//		return err // somebody else has it
//	}
//	defer lock.Release(context.Background())
//
//	// Commits only while the lock is still ours, in the same log entry.
//	resp, err := client.Transaction().
//		Guard(lock).
//		Queue("reports").Push(report).
//		Commit(ctx)
//	if err == nil && resp.IsKVPrecondition() {
//		// The lock is somebody else's now; nothing was pushed.
//	}
//
// WHAT IT IS. A permit is one KV row in the namespace `queen-locks`, written
// with a lifetime: acquire is a putIfAbsent, renew a put with expect, release a
// delete with expect. The broker's POST /api/v1/locks does that turning, so
// every client shares one implementation. A lock is the semaphore of one
// permit; Queen.Semaphore is the same thing with more.
//
// WHAT IT IS NOT: A MUTEX. A permit EXPIRES, and nobody tells its holder. A
// process that is paused, partitioned or slow keeps running past its lifetime
// while somebody else acquires. So the lock alone never makes two holders
// impossible; what makes their WORK exclusive is the token:
//
//   - inside Queen, TransactionBuilder.Guard: the acks, pushes, KV writes and
//     timers of a step commit only if the permit is still this holder's. A
//     holder that was replaced commits nothing.
//   - outside Queen, Lock.Token: a number that only rises on a lock. A resource
//     that remembers the highest token it accepted and refuses a lower one
//     refuses the holder that was replaced. Accept an EQUAL one: a holder
//     writes many times with one token.
//
// THE TOKEN CHANGES AT EVERY RENEW. A renew rewrites the row, so the broker
// answers a new token and the one before stops working. A Lock keeps the
// current one: call Token and Guard when you use them, do not keep a copy.
//
// THE OWNER is the holder's identity, minted per handle. With it a call is safe
// to send again when its answer was lost: the broker answers the permit the
// first attempt took. Two handles with one owner are one holder; pass your own
// only if that is what you mean.

// LockNamespace is the KV namespace the permits live in. An ordinary one: the
// rows can be read and listed through the KV API, and a stuck lock an operator
// wants gone is a KV delete.
const LockNamespace = "queen-locks"

// Why a lock operation did not do what it asked. Closed taxonomy.
const (
	LockReasonHeld      = "held"      // acquire: every permit is held
	LockReasonContended = "contended" // acquire of a semaphore: free permits, all lost in a race; come back
	LockReasonLost      = "lost"      // renew, release: the token is no longer the row's
)

// ErrLockNotHeld is returned where something needed a held lock and the handle
// holds none: a guard was asked of it.
var ErrLockNotHeld = errors.New("queen: the lock is not held")

// ---------------------------------------------------------------------------
// The wire. The JSON tags ARE the contract.
// ---------------------------------------------------------------------------

// LockOp is one operation of a locks call. Build it with the Lock*Op
// constructors below.
type LockOp struct {
	Op         string `json:"op"`
	Name       string `json:"name"`
	TTLSeconds int64  `json:"ttlSeconds,omitempty"`
	Owner      string `json:"owner,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	Slot       int    `json:"slot,omitempty"`
	Token      int64  `json:"token,omitempty"`

	err error
}

// lockTTLSeconds is the lifetime in whole seconds, rounded UP. There is no
// "forever" and no default: a lock that never expires is one nobody can take
// back from a holder that died.
func lockTTLSeconds(ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		return 0, errors.New("queen: a lock needs a lifetime above zero; a holder that needs longer renews")
	}
	return int64(math.Ceil(ttl.Seconds())), nil
}

// LockAcquireOp takes a permit for ttl. A lock, unless WithLimit makes it a
// semaphore.
func LockAcquireOp(name string, ttl time.Duration) LockOp {
	secs, err := lockTTLSeconds(ttl)
	return LockOp{Op: "acquire", Name: name, TTLSeconds: secs, err: err}
}

// LockRenewOp extends the permit token names for ttl more. It answers a NEW
// token.
func LockRenewOp(name string, token int64, ttl time.Duration) LockOp {
	secs, err := lockTTLSeconds(ttl)
	if err == nil && token <= 0 {
		err = errors.New("queen: renew needs the token of the permit (the one the last acquire or renew answered)")
	}
	return LockOp{Op: "renew", Name: name, Token: token, TTLSeconds: secs, err: err}
}

// LockReleaseOp gives the permit token names back.
func LockReleaseOp(name string, token int64) LockOp {
	var err error
	if token <= 0 {
		err = errors.New("queen: release needs the token of the permit (the one the last acquire or renew answered)")
	}
	return LockOp{Op: "release", Name: name, Token: token, err: err}
}

// LockGetOp asks who holds it.
func LockGetOp(name string) LockOp {
	return LockOp{Op: "get", Name: name}
}

// WithOwner names the holder (acquire, renew).
func (o LockOp) WithOwner(owner string) LockOp {
	o.Owner = owner
	return o
}

// WithLimit makes an acquire a semaphore's, of limit permits. Every caller of
// one name sends the same limit: it is the caller's and is stored nowhere.
func (o LockOp) WithLimit(limit int) LockOp {
	o.Limit = limit
	return o
}

// WithSlot names the slot the permit is in (renew, release). Zero, the only
// slot a lock has, is the default and is not sent.
func (o LockOp) WithSlot(slot int) LockOp {
	o.Slot = slot
	return o
}

// LockHolder is somebody holding a permit. A refused acquire and a lost renew
// or release name Slot and Owner; a get fills every field.
type LockHolder struct {
	Slot int `json:"slot"`
	// Owner is empty for a permit taken without one, and for a row somebody
	// wrote by hand through the KV API.
	Owner string `json:"owner"`
	Token int64  `json:"token"`
	// Since is when the holder took the permit; a renewal does not move it.
	Since     time.Time `json:"since"`
	ExpiresAt time.Time `json:"expiresAt"`
	// RenewedAt is when the permit was last acquired or renewed.
	RenewedAt time.Time `json:"renewedAt"`
}

// LockResult is one element of the answer, index-aligned to the operation that
// produced it. Each operation sets its own verdict and leaves the others
// false: Acquired for an acquire, Renewed for a renew, Released for a release,
// Held for a get.
//
// A verdict is a field, never an error: a lock somebody else holds is
// Acquired:false with a nil error.
type LockResult struct {
	Index int    `json:"index"`
	Op    string `json:"op"`
	Name  string `json:"name"`

	Acquired bool `json:"acquired"`
	// Already: the owner held this permit before the call (the answer to a
	// retry whose first attempt had won).
	Already  bool `json:"already"`
	Renewed  bool `json:"renewed"`
	Released bool `json:"released"`
	Held     bool `json:"held"`

	// Reason is one of the LockReason* constants when the verdict is false.
	Reason string `json:"reason"`
	Slot   int    `json:"slot"`
	// Token is the fencing token of the lease period this answer opens: on one
	// lock a later one is always higher. Set when a permit was granted or
	// renewed, and it replaces every token before it.
	Token int64  `json:"token"`
	Owner string `json:"owner"`
	// Guard is the KV check that holds while the permit is the caller's, ready
	// for TransactionBuilder.KV or KV.Batch.
	Guard   *KVOp        `json:"guard"`
	Holders []LockHolder `json:"holders"`
}

type lockBatchRequest struct {
	Operations []LockOp `json:"operations"`
}

// Locks is the four lock operations as the broker speaks them, with no state
// kept: the caller carries the token. Queen.Lock is what most code wants; this
// is for Get (who holds it?) and for a caller with its own loop.
type Locks struct {
	httpClient *HttpClient
}

// Locks returns the stateless locks API.
func (q *Queen) Locks() *Locks {
	return &Locks{httpClient: q.httpClient}
}

// Batch sends several operations in one call, each on a different lock: one
// result per operation, in order. They are independent; nothing here is
// all-or-nothing.
func (l *Locks) Batch(ctx context.Context, ops ...LockOp) ([]LockResult, error) {
	if len(ops) == 0 {
		return nil, errors.New("queen: a locks call needs at least one operation")
	}
	for i, op := range ops {
		if op.err != nil {
			return nil, fmt.Errorf("queen: lock operation at index %d: %w", i, op.err)
		}
	}
	body, err := l.httpClient.PostRaw(ctx, "/api/v1/locks", lockBatchRequest{Operations: ops})
	if err != nil {
		return nil, surfaceError(err)
	}
	var out struct {
		Results []LockResult `json:"results"`
	}
	if err := decodeKV(body, &out); err != nil {
		return nil, fmt.Errorf("queen: locks response: %w", err)
	}
	if len(out.Results) != len(ops) {
		return nil, fmt.Errorf("queen: locks returned %d results for %d operations", len(out.Results), len(ops))
	}
	return out.Results, nil
}

// Send sends one operation.
func (l *Locks) Send(ctx context.Context, op LockOp) (LockResult, error) {
	res, err := l.Batch(ctx, op)
	if err != nil {
		return LockResult{}, err
	}
	return res[0], nil
}

// Get answers who holds the lock: Held, and the holders by slot.
func (l *Locks) Get(ctx context.Context, name string) (LockResult, error) {
	return l.Send(ctx, LockGetOp(name))
}

// ---------------------------------------------------------------------------
// The handle.
// ---------------------------------------------------------------------------

// LockOptions tunes a Lock. The zero value is the default.
type LockOptions struct {
	// Owner is the holder's identity, instead of the one minted per handle.
	// Two handles with one owner are one holder.
	Owner string
	// ManualRenew turns the background renewal off: the caller calls Renew.
	ManualRenew bool
	// RenewEvery replaces "every third of the lifetime".
	RenewEvery time.Duration
	// RetryMin and RetryMax shape how a waiting Acquire comes back: first
	// after RetryMin (100ms), then half as long again each time up to RetryMax
	// (1s), with jitter.
	RetryMin time.Duration
	RetryMax time.Duration
}

type heldPermit struct {
	token int64
	slot  int
	// The broker's own guard for this lease period, kept as answered: where a
	// permit's row lives is the broker's rule, written once, there.
	guard      KVOp
	validUntil time.Time
}

// Lock is one holder's hold on one lock, or on one permit of a semaphore.
//
// It keeps the token current, renews in the background (every third of the
// lifetime, unless LockOptions.ManualRenew) and says when the permit is gone:
// the channel Lost returns is closed. "Gone" is the broker saying so, or the
// lifetime passing on THIS machine's clock with no renew having succeeded -- a
// client that cannot reach the broker has to assume the worst.
//
// Safe for concurrent use.
type Lock struct {
	locks *Locks
	name  string
	limit int
	owner string
	ttl   time.Duration
	opts  LockOptions

	// One renew at a time, and what a guard waits on to read a settled token.
	renewMu sync.Mutex

	mu    sync.Mutex
	held  *heldPermit
	epoch uint64        // bumped at every acquire, release and loss
	lost  chan struct{} // closed when the current permit is lost
	stop  chan struct{} // stops the keeper of the current permit
}

func newLock(locks *Locks, name string, limit int, ttl time.Duration, opts []LockOptions) *Lock {
	var o LockOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	if o.Owner == "" {
		o.Owner = mintLockOwner()
	}
	if o.RetryMin <= 0 {
		o.RetryMin = 100 * time.Millisecond
	}
	if o.RetryMax < o.RetryMin {
		o.RetryMax = time.Second
		if o.RetryMax < o.RetryMin {
			o.RetryMax = o.RetryMin
		}
	}
	return &Lock{
		locks: locks,
		name:  name,
		limit: limit,
		owner: o.Owner,
		ttl:   ttl,
		opts:  o,
		lost:  make(chan struct{}),
	}
}

func mintLockOwner() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "host"
	}
	if len(host) > 128 {
		host = host[:128]
	}
	var b [6]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}

// Lock returns a handle on a lock: one holder at a time, held as a lease of
// ttl, with a fencing token. Creating it sends nothing.
//
// It is a lease, not a mutex: it expires, and a holder that outlived it keeps
// running. TransactionBuilder.Guard is what makes the WORK exclusive.
func (q *Queen) Lock(name string, ttl time.Duration, opts ...LockOptions) *Lock {
	return newLock(q.Locks(), name, 1, ttl, opts)
}

// Semaphore returns a handle on ONE permit of a semaphore of limit permits;
// make a handle per holder. Every holder of one name passes the same limit --
// it is the caller's and is stored nowhere, so while a limit is being changed
// the larger one rules.
func (q *Queen) Semaphore(name string, limit int, ttl time.Duration, opts ...LockOptions) *Lock {
	return newLock(q.Locks(), name, limit, ttl, opts)
}

// Name is the lock's name.
func (l *Lock) Name() string { return l.name }

// Owner is this handle's identity.
func (l *Lock) Owner() string { return l.owner }

func (l *Lock) current() *heldPermit {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held == nil || !time.Now().Before(l.held.validUntil) {
		return nil
	}
	h := *l.held
	return &h
}

// Held reports whether this handle holds a permit, as far as it can know: the
// broker granted or renewed it, and its lifetime has not run out on this
// machine's clock. A belief with a deadline, not a proof -- the proof is the
// guard on the transaction.
func (l *Lock) Held() bool { return l.current() != nil }

// Token is the fencing token of the current lease period, or 0 when not held.
func (l *Lock) Token() int64 {
	if h := l.current(); h != nil {
		return h.token
	}
	return 0
}

// Slot is the semaphore slot this handle holds (0 for a lock), or -1 when not
// held.
func (l *Lock) Slot() int {
	if h := l.current(); h != nil {
		return h.slot
	}
	return -1
}

// Guard is the KV operation that holds while the permit is this handle's: a
// check of the permit's row at the current token, Required.
// TransactionBuilder.Guard adds it and follows a renew; take this one at the
// moment you send it, for a KV batch of your own.
func (l *Lock) Guard() (KVOp, error) {
	h := l.current()
	if h == nil {
		return KVOp{}, fmt.Errorf("%w: %q has nothing to guard with", ErrLockNotHeld, l.name)
	}
	return h.guard, nil
}

// Lost returns a channel that is closed when the permit is lost: the broker
// refused a renew or a guard, or the lifetime ran out here with no renew having
// succeeded. A release is not a loss. The channel belongs to the current hold;
// ask again after the next acquire.
func (l *Lock) Lost() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lost
}

// TryAcquire takes the permit, once. false when somebody else holds it.
func (l *Lock) TryAcquire(ctx context.Context) (bool, error) {
	if l.Held() {
		return true, nil
	}
	op := LockAcquireOp(l.name, l.ttl).WithOwner(l.owner)
	if l.limit > 1 {
		op = op.WithLimit(l.limit)
	}
	sent := time.Now()
	r, err := l.locks.Send(ctx, op)
	if err != nil {
		return false, err
	}
	if !r.Acquired {
		return false, nil
	}
	if err := l.take(r, sent); err != nil {
		return false, err
	}
	return true, nil
}

// Acquire takes the permit, coming back until it is free or ctx is done: the
// context is the wait. nil means held; ctx.Err() means the wait ended first.
func (l *Lock) Acquire(ctx context.Context) error {
	pause := l.opts.RetryMin
	for {
		ok, err := l.TryAcquire(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		// A random quarter off, so a crowd of waiters spreads.
		jittered := time.Duration(float64(pause) * (0.75 + mrand.Float64()*0.25))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(jittered):
		}
		pause = time.Duration(float64(pause) * 1.5)
		if pause > l.opts.RetryMax {
			pause = l.opts.RetryMax
		}
	}
}

// Renew extends the lease now. true with a new token in place, or false: the
// permit is gone and the handle says so. An error means the broker could not
// be asked -- the permit is then neither renewed nor known lost, and its
// deadline stands.
func (l *Lock) Renew(ctx context.Context) (bool, error) {
	l.renewMu.Lock()
	defer l.renewMu.Unlock()

	l.mu.Lock()
	if l.held == nil {
		l.mu.Unlock()
		return false, nil
	}
	held, epoch := *l.held, l.epoch
	l.mu.Unlock()

	sent := time.Now()
	r, err := l.locks.Send(ctx, LockRenewOp(l.name, held.token, l.ttl).WithSlot(held.slot).WithOwner(l.owner))
	if err != nil {
		return false, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	// Released, or lost, while the renew was in flight: its answer is about a
	// permit this handle no longer has.
	if l.epoch != epoch {
		return false, nil
	}
	if !r.Renewed {
		l.loseLocked("renew")
		return false, nil
	}
	h, err := permitOf(r, sent, l.ttl)
	if err != nil {
		return false, err
	}
	l.held = h
	return true, nil
}

// Release gives the permit back. true when the broker removed it; false when
// it was not this handle's any more, or never held. Either way the handle
// holds nothing afterwards and can acquire again.
func (l *Lock) Release(ctx context.Context) (bool, error) {
	// A renew in flight owns the token until it answers.
	l.renewMu.Lock()
	defer l.renewMu.Unlock()

	l.mu.Lock()
	held := l.held
	l.dropLocked()
	l.mu.Unlock()
	if held == nil {
		return false, nil
	}
	r, err := l.locks.Send(ctx, LockReleaseOp(l.name, held.token).WithSlot(held.slot))
	if err != nil {
		return false, err
	}
	return r.Released, nil
}

// settled waits out a renew in flight: the token is then the current one.
func (l *Lock) settled() {
	l.renewMu.Lock()
	l.renewMu.Unlock() //nolint:staticcheck // an empty critical section is the point
}

// markLost records that the broker said the permit is not this handle's.
func (l *Lock) markLost(reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held != nil {
		l.loseLocked(reason)
	}
}

func permitOf(r LockResult, sent time.Time, ttl time.Duration) (*heldPermit, error) {
	if r.Token <= 0 || r.Guard == nil {
		return nil, errors.New("queen: a granted permit must carry its token and its guard")
	}
	secs, err := lockTTLSeconds(ttl)
	if err != nil {
		return nil, err
	}
	return &heldPermit{
		token: r.Token,
		slot:  r.Slot,
		guard: *r.Guard,
		// Counted from when the request was SENT, so it is never later than
		// the broker's own deadline.
		validUntil: sent.Add(time.Duration(secs) * time.Second),
	}, nil
}

func (l *Lock) take(r LockResult, sent time.Time) error {
	h, err := permitOf(r, sent, l.ttl)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.dropLocked()
	l.held = h
	l.lost = make(chan struct{})
	l.stop = make(chan struct{})
	go l.keep(l.epoch, l.stop)
	return nil
}

// dropLocked forgets the permit and stops its keeper. l.mu is held.
func (l *Lock) dropLocked() {
	l.held = nil
	l.epoch++
	if l.stop != nil {
		close(l.stop)
		l.stop = nil
	}
}

// loseLocked is dropLocked plus the announcement. l.mu is held.
func (l *Lock) loseLocked(reason string) {
	logWarn("Lock.lost", map[string]interface{}{"lock": l.name, "owner": l.owner, "reason": reason})
	lost := l.lost
	l.dropLocked()
	select {
	case <-lost:
	default:
		close(lost)
	}
}

// keep is the background of one hold: renew when due, and call the permit lost
// when its lifetime runs out here.
func (l *Lock) keep(epoch uint64, stop <-chan struct{}) {
	every := l.opts.RenewEvery
	if every <= 0 {
		every = l.ttl / 3
	}
	pause := every
	for {
		l.mu.Lock()
		if l.epoch != epoch || l.held == nil {
			l.mu.Unlock()
			return
		}
		left := time.Until(l.held.validUntil)
		l.mu.Unlock()

		wait := left
		if !l.opts.ManualRenew && pause < left {
			wait = pause
		}
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		}

		l.mu.Lock()
		if l.epoch != epoch || l.held == nil {
			l.mu.Unlock()
			return
		}
		if !time.Now().Before(l.held.validUntil) {
			l.loseLocked("expired")
			l.mu.Unlock()
			return
		}
		l.mu.Unlock()
		if l.opts.ManualRenew {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), every)
		ok, err := l.Renew(ctx)
		cancel()
		switch {
		case err != nil:
			// Could not ask. Not a loss yet: come back sooner, until the
			// deadline above calls it.
			logWarn("Lock.renew", map[string]interface{}{"lock": l.name, "error": err.Error()})
			pause = every / 4
			if pause < 50*time.Millisecond {
				pause = 50 * time.Millisecond
			}
		case !ok:
			return
		default:
			pause = every
		}
	}
}
