package compat

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// The live half of M4: the group behaviours that only a running facade, a
// running broker and a real client can be made to disagree about — a member
// dying without saying so, offsets outliving the process that served them, a
// member joining and leaving three times in a row.
//
// Everything here uses the EAGER protocol (range, then round-robin) rather than
// franz-go's default cooperative-sticky: eager is what revokes the whole
// assignment on every rebalance, so it is the protocol that actually exercises
// the coordinator's generation handling, and it is what the older clients in
// the M6 matrix speak.
//
// The session timeout is pinned low (6 s, the facade's
// QUEEN_KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS) because one test waits one out.
// franz-go's default is 45 s, which would make that test five times slower
// without testing anything else.

const liveSession = 6 * time.Second

// eagerGroup is the option set every test in this file shares: a named group,
// one topic, from the beginning, with cooperative rebalancing OFF.
func eagerGroup(group, topic string, extra ...kgo.Opt) []kgo.Opt {
	return append([]kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.Balancers(kgo.RangeBalancer(), kgo.RoundRobinBalancer()),
		kgo.SessionTimeout(liveSession),
		kgo.HeartbeatInterval(time.Second),
	}, extra...)
}

// CHECK 1. One group member reads a whole topic, commits, and closes cleanly.
//
// The commit is asserted through OffsetFetch rather than through the client
// that made it: what a consumer believes it committed is not evidence, and the
// sum of the committed offsets is exactly the number of records read when every
// partition starts at 0.
func TestGroupSingleMemberConsumesCommitsAndExits(t *testing.T) {
	perPartition := 200 / int(topicWidth(t))
	topic, total := seedAcrossPartitions(t, perPartition)
	group := groupName(t)

	cl := newClient(t, eagerGroup(group, topic, kgo.DisableAutoCommit())...)
	got := drain(t, cl, total, 120*time.Second)
	if err := cl.CommitRecords(ctxFor(t, 30*time.Second), got...); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// A clean exit: LeaveGroup, and nothing left blocked. Close does not report,
	// so the assertion is that it returns at all — a coordinator that never
	// answered the leave would hang here until the test's own timeout.
	done := make(chan struct{})
	go func() { cl.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Close() did not return within 30s: the member never left the group")
	}

	if len(got) != total {
		t.Fatalf("the member read %d records, want %d", len(got), total)
	}
	keys := map[string]int{}
	for _, r := range got {
		keys[string(r.Key)]++
	}
	if len(keys) != total {
		t.Errorf("%d distinct records, want %d (duplicates: %d)", len(keys), total, total-len(keys))
	}

	committed := fetchOffsets(t, group, topic)
	var sum int64
	for p, off := range committed {
		if off < 0 {
			t.Errorf("partition %d has no committed offset after a full read: %d", p, off)
			continue
		}
		sum += off
	}
	if sum != int64(total) {
		t.Errorf("committed offsets sum to %d, want %d: %v", sum, total, committed)
	}
}

// CHECK 2. Two members, one group, eight partitions, the EAGER protocol: the
// assignment is a partition of the topic, not a copy of it.
//
// The pre-existing TestConsumerGroupTwoMembersSplitThePartitions asserts the
// same delivery property under franz-go's default cooperative-sticky balancer.
// This one pins the range/round-robin path and, because it watches the
// assignment callbacks rather than inferring ownership from what arrived, it
// says what the split WAS: 4 and 4, from an assignor the facade never looks
// inside.
func TestGroupTwoMembersSplitThePartitionsEagerly(t *testing.T) {
	topic, total := seedAcrossPartitions(t, 5)
	group := groupName(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var mu sync.Mutex
	owners := make(map[int32]map[int]bool) // partition -> members that delivered it
	assigned := make(map[int]map[int32]bool)
	seen := make(map[string]int)
	delivered := 0

	var wg sync.WaitGroup
	for member := 0; member < 2; member++ {
		member := member
		cl := newClient(t, eagerGroup(group, topic,
			kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
				mu.Lock()
				defer mu.Unlock()
				if assigned[member] == nil {
					assigned[member] = make(map[int32]bool)
				}
				for _, ps := range m {
					for _, p := range ps {
						assigned[member][p] = true
					}
				}
			}))...)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				fs := cl.PollRecords(ctx, 50)
				if ctx.Err() != nil {
					return
				}
				if errs := fs.Errors(); len(errs) > 0 {
					for _, e := range errs {
						if !isRebalanceish(e.Err) {
							t.Errorf("member %d: fetch error: %v", member, e.Err)
							return
						}
					}
				}
				mu.Lock()
				fs.EachRecord(func(r *kgo.Record) {
					if owners[r.Partition] == nil {
						owners[r.Partition] = make(map[int]bool)
					}
					owners[r.Partition][member] = true
					seen[string(r.Key)]++
					delivered++
				})
				done := delivered >= total
				mu.Unlock()
				if done {
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()

	if delivered < total {
		t.Fatalf("the group delivered %d/%d records before the budget ran out", delivered, total)
	}
	for key, times := range seen {
		if times != 1 {
			t.Errorf("record %s was delivered %d times", key, times)
		}
	}
	// Both members worked, and no partition was read by both.
	for partition, members := range owners {
		if len(members) != 1 {
			t.Errorf("partition %d was read by %d members, want 1", partition, len(members))
		}
	}
	if got := int32(len(owners)); got != topicWidth(t) {
		t.Errorf("%d partitions were consumed, want %d", got, topicWidth(t))
	}
	// ...and the assignment itself, as the members were told it. Range over 8
	// partitions and 2 members is 4/4; any assignor that gave one member
	// everything would still deliver every record, and this is what says so.
	if len(assigned) != 2 {
		t.Fatalf("%d of the 2 members were assigned anything: %v", len(assigned), assigned)
	}
	for member, ps := range assigned {
		t.Logf("member %d was assigned %d partitions: %v", member, len(ps), sortedPartitions(ps))
		if len(ps) == 0 || int32(len(ps)) == topicWidth(t) {
			t.Errorf("member %d was assigned %d of %d partitions, which is not a split",
				member, len(ps), topicWidth(t))
		}
	}
}

// CHECK 3. A member DIES — no LeaveGroup, no last heartbeat, its sockets simply
// stop — and the group has to notice on its own.
//
// The kill is a dialler that closes every connection the doomed member holds
// and refuses to make it another one, which is what the coordinator sees when a
// pod is OOM-killed: the difference from `Close()` (the case
// TestConsumerGroupRebalancesWhenAMemberLeaves covers) is that nothing tells the
// coordinator anything, so the eviction has to come from the session timeout.
//
// Duplicates are legal here and are counted rather than forbidden: the dead
// member had read records it never committed, so at-least-once says the
// survivor reads them again. What is NOT legal is a record that nobody ever
// delivers.
func TestGroupRebalancesWhenAMemberDiesWithoutLeaving(t *testing.T) {
	topic, total := seedAcrossPartitions(t, 6)
	group := groupName(t)

	var mu sync.Mutex
	seen := map[string]int{}
	deliveries := 0
	collect := func(fs kgo.Fetches) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		fs.EachRecord(func(r *kgo.Record) {
			seen[string(r.Key)]++
			deliveries++
			n++
		})
		return n
	}
	distinct := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(seen)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// The doomed member. Its client is deliberately never Closed — a dead
	// process does not get to run a deferred function — so it is built by hand
	// rather than through newClient, which registers one.
	dead := &severable{}
	doomed, err := kgo.NewClient(append(baseOpts(t),
		eagerGroup(group, topic, kgo.Dialer(dead.dial))...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient (doomed): %v", err)
	}
	survivor := newClient(t, eagerGroup(group, topic)...)

	// Both members read something first, so the kill lands on a member that was
	// genuinely assigned partitions and genuinely mid-consumption.
	for _, m := range []struct {
		name string
		cl   *kgo.Client
	}{{"doomed", doomed}, {"survivor", survivor}} {
		pollCtx, pollCancel := context.WithTimeout(ctx, 90*time.Second)
		fs := m.cl.PollRecords(pollCtx, 5)
		pollCancel()
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("%s: fetch error before the kill: %v", m.name, errs)
		}
		if collect(fs) == 0 {
			t.Fatalf("%s was assigned nothing before the kill", m.name)
		}
	}
	before := distinct()
	t.Logf("both members read: %d distinct records before the kill", before)

	killedAt := time.Now()
	dead.sever()

	// The survivor now has to reach every record, including the ones the dead
	// member owned. It cannot until the coordinator evicts the corpse, so this
	// loop IS the assertion that the eviction happens.
	for distinct() < total {
		if ctx.Err() != nil {
			t.Fatalf("%s after the kill the survivor reached %d/%d distinct records",
				time.Since(killedAt).Round(time.Second), distinct(), total)
		}
		fs := survivor.PollRecords(ctx, 100)
		if ctx.Err() != nil {
			t.Fatalf("%s after the kill the survivor reached %d/%d distinct records",
				time.Since(killedAt).Round(time.Second), distinct(), total)
		}
		if errs := fs.Errors(); len(errs) > 0 {
			// A rebalance is not a fetch failure. Anything else is.
			for _, e := range errs {
				if !isRebalanceish(e.Err) {
					t.Fatalf("fetch error after the kill: %v", e.Err)
				}
			}
		}
		collect(fs)
	}
	t.Logf("the survivor had every record %s after the kill; %d deliveries for %d records (%d duplicates, legal at-least-once)",
		time.Since(killedAt).Round(time.Second), deliveries, total, deliveries-total)

	if distinct() != total {
		t.Errorf("%d distinct records, want %d", distinct(), total)
	}
}

// CHECK 4. The committed offsets outlive the facade.
//
// The facade holds group MEMBERSHIP in memory and nothing else: offsets are in
// Queen. So a restart must look to a client exactly like a Kafka broker
// restart — rejoin, and resume from the commit — and the way to prove that is
// to write MORE records while nobody is consuming and then check that a fresh
// member sees those and only those.
func TestGroupOffsetsSurviveAFacadeRestart(t *testing.T) {
	restart := os.Getenv("QUEEN_KAFKA_RESTART_CMD")
	if restart == "" {
		t.Skip("no QUEEN_KAFKA_RESTART_CMD: run queen-kafka/compat/rig.sh, which sets it")
	}

	topic, total := seedAcrossPartitions(t, 5)
	group := groupName(t)

	first := newClient(t, eagerGroup(group, topic, kgo.DisableAutoCommit())...)
	got := drain(t, first, total, 120*time.Second)
	if err := first.CommitRecords(ctxFor(t, 30*time.Second), got...); err != nil {
		t.Fatalf("commit: %v", err)
	}
	first.Close()

	// 50 more, produced while the group is stopped, so what a resumed member
	// reads is unambiguous.
	const extra = 50
	producer := newClient(t, kgo.RequiredAcks(kgo.AllISRAcks()))
	var more []*kgo.Record
	for i := 0; i < extra; i++ {
		more = append(more, &kgo.Record{
			Topic:     topic,
			Partition: int32(i) % topicWidth(t),
			Key:       []byte(fmt.Sprintf("late-%d", i)),
			Value:     []byte(fmt.Sprintf("late value %d", i)),
		})
	}
	produceSync(t, producer, more)
	producer.Close()

	// The restart itself: SIGKILL and a fresh process on the same port, which
	// is a crash and not a handover — every connection and all of the in-memory
	// group state goes with it.
	out, err := exec.CommandContext(ctxFor(t, 90*time.Second), restart).CombinedOutput()
	if err != nil {
		t.Fatalf("restarting the facade (%s): %v\n%s", restart, err, out)
	}
	// The script names the pid it killed and the one it started, because a
	// restart that silently did nothing would pass every assertion below.
	pids := strings.TrimSpace(string(out))
	old, fresh := pidField(pids, "old="), pidField(pids, "new=")
	if old == "" || fresh == "" || old == fresh || old == "none" {
		t.Fatalf("the facade was not actually restarted: %q", pids)
	}
	t.Logf("facade restarted: pid %s -> %s", old, fresh)

	// The commits are still there, read the way an admin tool reads them —
	// before any consumer has had the chance to re-commit anything.
	committed := fetchOffsets(t, group, topic)
	var sum int64
	for _, off := range committed {
		if off > 0 {
			sum += off
		}
	}
	if sum != int64(total) {
		t.Fatalf("after the restart the committed offsets sum to %d, want %d: %v",
			sum, total, committed)
	}

	// A brand-new member, told to start at the beginning: it must get the 50
	// late records and nothing else.
	second := newClient(t, eagerGroup(group, topic, kgo.DisableAutoCommit())...)
	rest := drain(t, second, extra, 120*time.Second)
	for _, r := range rest {
		if !strings.HasPrefix(string(r.Key), "late-") {
			t.Fatalf("record %q was replayed after the restart: the group did not resume from its commit",
				r.Key)
		}
	}

	// ...and then nothing more: a short poll that comes back empty is the
	// difference between "resumed" and "resumed, then replayed the rest".
	tail, tailCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer tailCancel()
	if fs := second.PollRecords(tail, 10); fs.NumRecords() > 0 {
		var extras []string
		fs.EachRecord(func(r *kgo.Record) { extras = append(extras, string(r.Key)) })
		t.Fatalf("%d records arrived after the 50 late ones: %v", len(extras), extras)
	}
}

// CHECK 5. A member that joins and leaves three times running does not wedge the
// group: the steady member keeps its assignment current and the topic still
// gets fully consumed, and every heartbeat error franz-go reports along the way
// is a rebalance and not a fencing.
func TestGroupSurvivesARejoinStorm(t *testing.T) {
	topic, total := seedAcrossPartitions(t, 4)
	group := groupName(t)

	logs := &recordingLogger{}
	steady := newClient(t, eagerGroup(group, topic, kgo.WithLogger(logs))...)

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	var mu sync.Mutex
	seen := map[string]bool{}
	poll := func(cl *kgo.Client, budget time.Duration) {
		pollCtx, pollCancel := context.WithTimeout(ctx, budget)
		defer pollCancel()
		fs := cl.PollRecords(pollCtx, 100)
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if !isRebalanceish(e.Err) && pollCtx.Err() == nil {
					t.Errorf("fetch error during the storm: %v", e.Err)
				}
			}
		}
		mu.Lock()
		fs.EachRecord(func(r *kgo.Record) { seen[string(r.Key)] = true })
		mu.Unlock()
	}

	poll(steady, 60*time.Second)
	if len(seen) == 0 {
		t.Fatal("the steady member read nothing before the storm")
	}

	for cycle := 1; cycle <= 3; cycle++ {
		transient := newClient(t, eagerGroup(group, topic, kgo.WithLogger(logs))...)
		// The joiner has to be admitted before it is any use as a disturbance:
		// polling it drives the join to completion.
		poll(transient, 60*time.Second)
		poll(steady, 20*time.Second)
		transient.Close()
		// ...and the group has to re-form after it goes. A poll that returns
		// without a fetch error is the steady member back in a stable
		// generation: a wedged one answers REBALANCE_IN_PROGRESS forever and
		// this loop's third cycle would never finish.
		poll(steady, 30*time.Second)
		t.Logf("cycle %d: %d/%d distinct records so far", cycle, len(seen), total)
	}

	for len(seen) < total {
		if ctx.Err() != nil {
			t.Fatalf("after the storm the group reached %d/%d records", len(seen), total)
		}
		poll(steady, 30*time.Second)
	}

	// What the client saw its heartbeats answered with. REBALANCE_IN_PROGRESS
	// is the whole point of a storm; a fencing (ILLEGAL_GENERATION,
	// UNKNOWN_MEMBER_ID) would mean the coordinator lost track of a member it
	// had just admitted.
	if bad := logs.badHeartbeats(); len(bad) > 0 {
		t.Errorf("heartbeat errors that are not REBALANCE_IN_PROGRESS-class:\n  %s",
			strings.Join(bad, "\n  "))
	}
	// Only the heartbeats that reported something: a debug logger writes one
	// line per beat per member, and a hundred "err=<nil>" lines would bury the
	// handful that are the point.
	beats, reported := logs.heartbeatOutcomes()
	t.Logf("%d heartbeat lines, %d of them carrying an error:", beats, len(reported))
	for _, line := range reported {
		t.Logf("  %s", line)
	}
}

// CHECK 6. A group that has never committed anything starts where its reset
// policy says, and the coordinator says "no offset" rather than "offset zero"
// on the way — the -1 that makes a client apply auto.offset.reset at all.
func TestFreshGroupOnAnExistingTopicStartsAtEarliest(t *testing.T) {
	topic, total := seedAcrossPartitions(t, 3)
	group := groupName(t)

	// Cold: every partition of a topic full of records answers -1.
	cold := fetchOffsets(t, group, topic)
	if len(cold) != int(topicWidth(t)) {
		t.Fatalf("OffsetFetch answered for %d partitions, want %d: %v",
			len(cold), topicWidth(t), cold)
	}
	for p, off := range cold {
		if off != -1 {
			t.Errorf("partition %d answered %d for a group that has committed nothing, want -1", p, off)
		}
	}

	// ...and a consumer that reads that -1 starts at the beginning, which for a
	// topic seeded from empty means every record.
	cl := newClient(t, eagerGroup(group, topic)...)
	got := drain(t, cl, total, 120*time.Second)
	if len(got) != total {
		t.Fatalf("the fresh group read %d records, want %d", len(got), total)
	}
	byPart := byPartition(got)
	for p, recs := range byPart {
		if recs[0].Offset != 0 {
			t.Errorf("partition %d started at offset %d, want 0", p, recs[0].Offset)
		}
	}
}

// ---------------------------------------------------------------- test plumbing

// baseOpts is what newClient builds on, spelled separately for the one client
// here that cannot use it: the doomed member needs a dialler of its own and
// must NOT have a Close registered, because a dead process does not get to run
// a deferred function.
func baseOpts(t *testing.T) []kgo.Opt {
	t.Helper()
	base := []kgo.Opt{
		kgo.SeedBrokers(bootstrap()),
		kgo.DisableIdempotentWrite(),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequestRetries(3),
	}
	if os.Getenv("COMPAT_VERBOSE") != "" {
		base = append(base, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}
	return base
}

// severable is how a member dies here: a dialler that hands out connections
// until it is severed, then closes the ones it handed out and refuses to make
// any more. From the coordinator's side that is indistinguishable from the
// process being killed — no LeaveGroup, no further heartbeats, and no
// reconnection to explain itself with.
type severable struct {
	mu      sync.Mutex
	severed bool
	conns   []net.Conn
}

func (s *severable) dial(ctx context.Context, network, host string) (net.Conn, error) {
	s.mu.Lock()
	dead := s.severed
	s.mu.Unlock()
	if dead {
		return nil, errors.New("this member is dead")
	}

	c, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, host)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.severed {
		// Severed while this dial was in flight.
		_ = c.Close()
		return nil, errors.New("this member is dead")
	}
	s.conns = append(s.conns, c)
	return c, nil
}

func (s *severable) sever() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.severed = true
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.conns = nil
}

// isRebalanceish: the errors a correct client reports while a group is moving,
// which are progress rather than failure. Matched on the text because franz-go
// wraps some of them in its own types and the point here is what a user would
// see in a log, not which struct carried it.
func isRebalanceish(err error) bool {
	if err == nil {
		return true
	}
	s := err.Error()
	for _, ok := range []string{
		"REBALANCE_IN_PROGRESS",
		"rebalance",
		"context canceled",
		"context deadline exceeded",
		"client closed",
	} {
		if strings.Contains(s, ok) {
			return true
		}
	}
	return false
}

// recordingLogger keeps every line franz-go writes, so a test can ask what the
// client's own group machinery reported rather than inferring it from
// behaviour. It is written to from several goroutines: the heartbeat loop, the
// fetch loop and the test's.
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) Level() kgo.LogLevel { return kgo.LogLevelDebug }

func (l *recordingLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var b strings.Builder
	fmt.Fprintf(&b, "[%s] %s", level, msg)
	for i := 0; i+1 < len(keyvals); i += 2 {
		fmt.Fprintf(&b, " %v=%v", keyvals[i], keyvals[i+1])
	}
	l.mu.Lock()
	l.lines = append(l.lines, b.String())
	l.mu.Unlock()
}

func (l *recordingLogger) heartbeatLines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, line := range l.lines {
		if strings.Contains(strings.ToLower(line), "heartbeat") {
			out = append(out, line)
		}
	}
	return out
}

// heartbeatOutcomes: how many heartbeat lines there were, and the ones that
// reported an error of any kind.
func (l *recordingLogger) heartbeatOutcomes() (int, []string) {
	lines := l.heartbeatLines()
	var reported []string
	for _, line := range lines {
		if err := heartbeatErr(line); err != "" {
			reported = append(reported, line)
		}
	}
	return len(lines), reported
}

// heartbeatErr pulls the err= field out of a heartbeat line, empty when the
// line carries none or carries a nil one.
func heartbeatErr(line string) string {
	i := strings.Index(line, "err=")
	if i < 0 {
		return ""
	}
	err := strings.TrimSpace(line[i+len("err="):])
	if err == "<nil>" || err == "nil" {
		return ""
	}
	return err
}

// badHeartbeats: heartbeat lines that carry an error which is not a rebalance
// and not the client shutting down.
func (l *recordingLogger) badHeartbeats() []string {
	var out []string
	for _, line := range l.heartbeatLines() {
		err := heartbeatErr(line)
		if err == "" {
			continue
		}
		if isRebalanceish(errors.New(err)) {
			continue
		}
		out = append(out, line)
	}
	return out
}

// pidField pulls "old=1234"/"new=1234" out of the restart script's one line of
// output.
func pidField(line, key string) string {
	i := strings.Index(line, key)
	if i < 0 {
		return ""
	}
	rest := line[i+len(key):]
	if j := strings.IndexAny(rest, " \t\n"); j >= 0 {
		rest = rest[:j]
	}
	return rest
}

// sortedPartitions renders a partition set the way a log line wants it.
func sortedPartitions(set map[int32]bool) []int32 {
	out := make([]int32, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
