// M7 F2: the groups-admin trio against a running facade — ListGroups,
// DescribeGroups and DeleteGroups, driven by franz-go's `kmsg` at the versions
// a real AdminClient negotiates.
//
// Every assertion here is about something a client or an operator ACTS on: a
// group that appears in a listing, the host and the assignment bytes a
// `--describe` prints, an error code a delete branches on, and — the one that
// matters most — the offsets that are actually gone afterwards.
//
// The behaviours asserted were measured against `apache/kafka:3.9.1` before
// this file was written (a group nobody has heard of is error 0 and `Dead`, a
// group with members refuses a delete with 68, a group nobody has heard of
// refuses one with 69), so a failure here is the facade disagreeing with the
// oracle rather than with an opinion.
package compat

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The Kafka error codes this file branches on, by name.
const (
	errInvalidGroupID  int16 = 24
	errNonEmptyGroup   int16 = 68
	errGroupIDNotFound int16 = 69
)

// The versions an AdminClient negotiates against the facade's advertised window
// (versions.rs: ListGroups 0-4, DescribeGroups 0-3, DeleteGroups 0-2).
const (
	listGroupsV     = 4
	describeGroupsV = 3
	deleteGroupsV   = 2
)

func listGroups(t *testing.T, cl *kgo.Client, version int16, states ...string) *kmsg.ListGroupsResponse {
	t.Helper()
	req := kmsg.NewPtrListGroupsRequest()
	req.SetVersion(version)
	req.StatesFilter = states
	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("ListGroups v%d: %v", version, err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("ListGroups v%d: error code %d", version, resp.ErrorCode)
	}
	return resp
}

// listedState answers the state the facade reports for `group`, or "" when the
// group is not in the listing at all.
func listedState(resp *kmsg.ListGroupsResponse, group string) string {
	for _, g := range resp.Groups {
		if g.Group == group {
			if g.GroupState == "" {
				return "<no state>"
			}
			return g.GroupState
		}
	}
	return ""
}

func describeGroups(t *testing.T, cl *kgo.Client, version int16, groups ...string) *kmsg.DescribeGroupsResponse {
	t.Helper()
	resp, err := tryDescribeGroups(cl, version, groups...)
	if err != nil {
		t.Fatalf("DescribeGroups v%d: %v", version, err)
	}
	return resp
}

func tryDescribeGroups(cl *kgo.Client, version int16, groups ...string) (*kmsg.DescribeGroupsResponse, error) {
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.SetVersion(version)
	req.Groups = groups
	req.IncludeAuthorizedOperations = version >= 3
	return req.RequestWith(context.Background(), cl)
}

// franz-go's high-level client shards the group-admin requests by coordinator
// and merges the answers, and a per-group error code comes back as an error on
// the WHOLE request rather than as a code in a result. So a test that asks
// about a group id the facade refuses has to read the code out of the error as
// well as out of the results.
//
// It never fires against a real Kafka for these names — Kafka answers `Dead`
// and GROUP_ID_NOT_FOUND for an empty or over-long group id, which are not
// errors — so this is the shape of the facade's own bound and is recorded as
// such in compat/ERRORS.md.
func groupAdminCode(err error) int16 {
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return ke.Code
	}
	return -1
}

func described(t *testing.T, resp *kmsg.DescribeGroupsResponse, group string) *kmsg.DescribeGroupsResponseGroup {
	t.Helper()
	for i := range resp.Groups {
		if resp.Groups[i].Group == group {
			return &resp.Groups[i]
		}
	}
	t.Fatalf("%s is not in the DescribeGroups answer", group)
	return nil
}

func deleteGroups(t *testing.T, cl *kgo.Client, version int16, groups ...string) *kmsg.DeleteGroupsResponse {
	t.Helper()
	resp, err := tryDeleteGroups(cl, version, groups...)
	if err != nil {
		t.Fatalf("DeleteGroups v%d: %v", version, err)
	}
	return resp
}

func tryDeleteGroups(cl *kgo.Client, version int16, groups ...string) (*kmsg.DeleteGroupsResponse, error) {
	req := kmsg.NewPtrDeleteGroupsRequest()
	req.SetVersion(version)
	req.Groups = groups
	return req.RequestWith(context.Background(), cl)
}

func deleteCode(t *testing.T, resp *kmsg.DeleteGroupsResponse, group string) int16 {
	t.Helper()
	for _, r := range resp.Groups {
		if r.Group == group {
			return r.ErrorCode
		}
	}
	t.Fatalf("%s is not in the DeleteGroups answer", group)
	return -1
}

// A group that has committed and has no members: exactly what a consumer fleet
// that has been stopped looks like, and the thing a registry-only listing
// cannot see.
func stoppedGroup(t *testing.T, topic string) string {
	t.Helper()
	group := groupName(t)
	cl := newClient(t, eagerGroup(group, topic, kgo.DisableAutoCommit())...)
	got := drain(t, cl, 1, 60*time.Second)
	if len(got) == 0 {
		t.Fatalf("%s read nothing, so it has nothing to commit", group)
	}
	if err := cl.CommitRecords(ctxFor(t, 30*time.Second), got...); err != nil {
		t.Fatalf("commit: %v", err)
	}
	cl.Close()
	return group
}

// CHECK G1. A group whose consumers are STOPPED is still listed.
//
// This is the whole reason the durable index exists: the group's actor is
// reaped five minutes after its last member leaves, and a registry-only answer
// would show nothing for exactly the group an operator opened the tool to look
// at.
func TestListGroupsShowsAStoppedGroupBesideALiveOne(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 2)
	stopped := stoppedGroup(t, topic)

	live := groupName(t)
	liveCl := newClient(t, eagerGroup(live, topic)...)
	drain(t, liveCl, 1, 60*time.Second)
	defer liveCl.Close()

	admin := newClient(t)
	resp := listGroups(t, admin, listGroupsV)
	if got := listedState(resp, stopped); got != "Empty" {
		t.Errorf("the stopped group %s lists as %q, want Empty", stopped, got)
	}
	if got := listedState(resp, live); got != "Stable" && got != "PreparingRebalance" && got != "CompletingRebalance" {
		t.Errorf("the live group %s lists as %q, want a live state", live, got)
	}
	// The protocol type is what `KafkaAdminClient.listConsumerGroups` reads to
	// decide a group is a consumer group at all.
	for _, g := range resp.Groups {
		if g.Group == live && g.ProtocolType != "consumer" {
			t.Errorf("the live group's protocol type is %q, want consumer", g.ProtocolType)
		}
	}
}

// CHECK G2. KIP-518's state filter is honoured, not ignored: a tool that asks
// for Stable must not be handed the stopped groups.
func TestListGroupsHonoursTheStateFilter(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 2)
	stopped := stoppedGroup(t, topic)

	live := groupName(t)
	liveCl := newClient(t, eagerGroup(live, topic)...)
	drain(t, liveCl, 1, 60*time.Second)
	defer liveCl.Close()

	admin := newClient(t)
	empties := listGroups(t, admin, listGroupsV, "Empty")
	if listedState(empties, stopped) != "Empty" {
		t.Errorf("--state Empty did not list the stopped group %s", stopped)
	}
	if s := listedState(empties, live); s != "" {
		t.Errorf("--state Empty listed the LIVE group %s as %q", live, s)
	}
	// A state string nothing is in answers an empty list and no error, which is
	// what Kafka 3.9.1 does with an unknown state too.
	none := listGroups(t, admin, listGroupsV, "Nonsense")
	if len(none.Groups) != 0 {
		t.Errorf("--state Nonsense listed %d groups", len(none.Groups))
	}
}

// CHECK G3. A live member is described with its identity and its two opaque
// byte strings — which is what makes `kafka-consumer-groups.sh --describe`
// print a members x partitions table at all.
func TestDescribeGroupsShowsMembersWithHostAndAssignment(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 2)
	group := groupName(t)

	cl := newClient(t, append(eagerGroup(group, topic), kgo.ClientID("qk-describe-probe"))...)
	drain(t, cl, 1, 60*time.Second)
	defer cl.Close()

	admin := newClient(t)
	g := described(t, describeGroups(t, admin, describeGroupsV, group), group)
	if g.ErrorCode != 0 {
		t.Fatalf("DescribeGroups %s: error code %d", group, g.ErrorCode)
	}
	if g.State != "Stable" && g.State != "CompletingRebalance" && g.State != "PreparingRebalance" {
		t.Errorf("state %q for a group with a live member", g.State)
	}
	if g.ProtocolType != "consumer" {
		t.Errorf("protocol type %q, want consumer", g.ProtocolType)
	}
	if len(g.Members) != 1 {
		t.Fatalf("%d members, want 1", len(g.Members))
	}
	m := g.Members[0]
	if m.ClientID != "qk-describe-probe" {
		t.Errorf("client id %q, want qk-describe-probe", m.ClientID)
	}
	// Kafka's own spelling, and the column an operator reads first.
	if len(m.ClientHost) < 2 || m.ClientHost[0] != '/' {
		t.Errorf("client host %q is not an address in Kafka's shape (/1.2.3.4)", m.ClientHost)
	}
	// The two byte strings, and the proof they are passed through rather than
	// invented: both DECODE with the client's own consumer-protocol reader, and
	// the assignment names the topic.
	meta := kmsg.NewConsumerMemberMetadata()
	if err := meta.ReadFrom(m.ProtocolMetadata); err != nil {
		t.Fatalf("the member metadata did not survive the facade: %v", err)
	}
	if len(meta.Topics) != 1 || meta.Topics[0] != topic {
		t.Errorf("the subscription says %v, want [%s]", meta.Topics, topic)
	}
	assign := kmsg.NewConsumerMemberAssignment()
	if err := assign.ReadFrom(m.MemberAssignment); err != nil {
		t.Fatalf("the member assignment did not survive the facade: %v", err)
	}
	var partitions int
	for _, at := range assign.Topics {
		if at.Topic == topic {
			partitions += len(at.Partitions)
		}
	}
	if partitions != int(topicWidth(t)) {
		t.Errorf("the sole member is assigned %d partitions, want %d", partitions, topicWidth(t))
	}
	// No ACL model, so no permission set is invented: Kafka's own
	// AUTHORIZED_OPERATIONS_OMITTED.
	if g.AuthorizedOperations != -2147483648 {
		t.Errorf("authorized_operations is %d, want the omitted sentinel", g.AuthorizedOperations)
	}
}

// CHECK G4. A group nobody has ever heard of is error 0 and `Dead`, and a group
// that has merely stopped is `Empty`. MEASURED against apache/kafka:3.9.1: the
// difference between the two is what tells an operator "your consumers are
// down" from "there is no such group".
func TestDescribeGroupsTellsAStoppedGroupFromAnUnknownOne(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 2)
	stopped := stoppedGroup(t, topic)
	admin := newClient(t)

	resp := describeGroups(t, admin, describeGroupsV, stopped, "qk-never-existed-"+newTopic(t))
	empty := described(t, resp, stopped)
	if empty.ErrorCode != 0 || empty.State != "Empty" {
		t.Errorf("the stopped group is (%d, %q), want (0, Empty)", empty.ErrorCode, empty.State)
	}
	for _, g := range resp.Groups {
		if g.Group == stopped {
			continue
		}
		if g.ErrorCode != 0 || g.State != "Dead" {
			t.Errorf("an unknown group is (%d, %q), want (0, Dead)", g.ErrorCode, g.State)
		}
		if len(g.Members) != 0 {
			t.Errorf("an unknown group has %d members", len(g.Members))
		}
	}
}

// CHECK G5. Kafka's rule, and the reason it exists: a `--delete` typed against
// a RUNNING fleet is refused and changes nothing.
func TestDeleteGroupsRefusesAGroupWithMembers(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 2)
	group := groupName(t)

	cl := newClient(t, eagerGroup(group, topic, kgo.DisableAutoCommit())...)
	got := drain(t, cl, 1, 60*time.Second)
	if err := cl.CommitRecords(ctxFor(t, 30*time.Second), got...); err != nil {
		t.Fatalf("commit: %v", err)
	}
	defer cl.Close()

	before := fetchOffsets(t, group, topic)
	admin := newClient(t)
	if code := deleteCode(t, deleteGroups(t, admin, deleteGroupsV, group), group); code != errNonEmptyGroup {
		t.Fatalf("deleting a live group answered %d, want NON_EMPTY_GROUP (%d)", code, errNonEmptyGroup)
	}
	after := fetchOffsets(t, group, topic)
	for p, off := range before {
		if after[p] != off {
			t.Errorf("a refused delete moved partition %d from %d to %d", p, off, after[p])
		}
	}
}

// CHECK G6. The whole delete, end to end: a stopped group's offsets are gone
// from Queen, the group reads back Dead, it disappears from the listing, and a
// fresh consumer of the same id replays from `auto.offset.reset`.
//
// That last clause is the footgun the design says out loud, and it is asserted
// rather than described.
func TestDeleteGroupsRemovesAStoppedGroupAndItsOffsets(t *testing.T) {
	topic, total := seedAcrossPartitions(t, 2)
	group := groupName(t)

	first := newClient(t, eagerGroup(group, topic, kgo.DisableAutoCommit())...)
	got := drain(t, first, total, 120*time.Second)
	if err := first.CommitRecords(ctxFor(t, 30*time.Second), got...); err != nil {
		t.Fatalf("commit: %v", err)
	}
	first.Close()

	committed := fetchOffsets(t, group, topic)
	var sum int64
	for _, off := range committed {
		if off > 0 {
			sum += off
		}
	}
	if sum != int64(total) {
		t.Fatalf("the fixture committed %d, want %d", sum, total)
	}

	admin := newClient(t)
	if code := deleteCode(t, deleteGroups(t, admin, deleteGroupsV, group), group); code != 0 {
		t.Fatalf("deleting a stopped group answered %d, want 0", code)
	}

	// Gone from the store: every partition reads back as never committed, which
	// is offset -1 and NOT an error.
	for p, off := range fetchOffsets(t, group, topic) {
		if off != -1 {
			t.Errorf("partition %d still has committed offset %d after the delete", p, off)
		}
	}
	// Gone from the listing, and Dead when described.
	if s := listedState(listGroups(t, admin, listGroupsV), group); s != "" {
		t.Errorf("a deleted group still lists, as %q", s)
	}
	g := described(t, describeGroups(t, admin, describeGroupsV, group), group)
	if g.ErrorCode != 0 || g.State != "Dead" {
		t.Errorf("a deleted group describes as (%d, %q), want (0, Dead)", g.ErrorCode, g.State)
	}
	// ...and the footgun, proved: the same group id replays the topic.
	second := newClient(t, eagerGroup(group, topic)...)
	defer second.Close()
	replayed := drain(t, second, total, 120*time.Second)
	if len(replayed) != total {
		t.Errorf("after a delete the group read %d records, want a full replay of %d", len(replayed), total)
	}
}

// CHECK G7. A group nobody has heard of is GROUP_ID_NOT_FOUND, a delete is
// idempotent (which is what makes a partially failed one re-runnable), and a
// group id that is not a name is refused without touching anything.
func TestDeleteGroupsAnswersForNamesItDoesNotHave(t *testing.T) {
	admin := newClient(t)
	never := "qk-never-existed-" + newTopic(t)
	if code := deleteCode(t, deleteGroups(t, admin, deleteGroupsV, never), never); code != errGroupIDNotFound {
		t.Errorf("an unknown group answered %d, want GROUP_ID_NOT_FOUND (%d)", code, errGroupIDNotFound)
	}
	// Empty and over-long ids: this facade bounds a group id at 255 characters
	// because every copy of one is its own, and INVALID_GROUP_ID is the code
	// every client already treats as "fix your configuration".
	huge := make([]byte, 256)
	for i := range huge {
		huge[i] = 'g'
	}
	for _, bad := range []string{"", string(huge)} {
		resp, err := tryDeleteGroups(admin, deleteGroupsV, bad)
		if err != nil {
			if code := groupAdminCode(err); code != errInvalidGroupID {
				t.Errorf("group id %q failed with %v (code %d), want INVALID_GROUP_ID (%d)", bad, err, code, errInvalidGroupID)
			}
			continue
		}
		for _, r := range resp.Groups {
			if r.ErrorCode != errInvalidGroupID {
				t.Errorf("group id %q answered %d, want INVALID_GROUP_ID (%d)", r.Group, r.ErrorCode, errInvalidGroupID)
			}
		}
	}
}

// CHECK G8. One request, several groups: the answers line up name for name and
// a refusal does not take its neighbours with it. Neither API has a top-level
// error field, so this is the only thing a client can rely on.
func TestGroupAdminAnswersLineUpWithTheRequest(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 2)
	stopped := stoppedGroup(t, topic)
	never := "qk-never-existed-" + newTopic(t)
	admin := newClient(t)

	names := []string{stopped, never}
	desc := describeGroups(t, admin, describeGroupsV, names...)
	if len(desc.Groups) != len(names) {
		t.Fatalf("DescribeGroups answered %d results for %d groups", len(desc.Groups), len(names))
	}
	var got []string
	for _, g := range desc.Groups {
		got = append(got, g.Group)
	}
	want := append([]string(nil), names...)
	sort.Strings(got)
	sort.Strings(want)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("DescribeGroups answered for %v, want %v", got, want)
		}
	}
	if described(t, desc, stopped).ErrorCode != 0 {
		t.Errorf("a group that exists was refused beside one that does not")
	}
	if described(t, desc, never).State != "Dead" {
		t.Errorf("an unknown group beside a real one describes as %q, want Dead", described(t, desc, never).State)
	}
	// The refused name goes in its own request: franz-go turns a per-group
	// refusal into a request-level error (see groupAdminCode), so asking about
	// a good group and a refused one together would say nothing about either.
	if _, err := tryDescribeGroups(admin, describeGroupsV, ""); err != nil {
		if code := groupAdminCode(err); code != errInvalidGroupID {
			t.Errorf("an empty group id failed with %v (code %d), want INVALID_GROUP_ID", err, code)
		}
	} else if c := described(t, describeGroups(t, admin, describeGroupsV, ""), "").ErrorCode; c != errInvalidGroupID {
		t.Errorf("an empty group id answered %d, want INVALID_GROUP_ID (%d)", c, errInvalidGroupID)
	}
}

// CHECK G9. The advertised window, read the way every client reads it.
//
// It is NOT a per-version walk, and that is a property of the client rather
// than a gap: franz-go rewrites the version of everything it sends to the
// highest the broker advertises, so a `SetVersion(0)` here goes out at 4
// anyway. The version-by-version checks live where they can be made — the
// crate's own dispatch walk (`conn.rs`) and the differential runner, which
// writes frames itself — and what a real client can assert is this: the
// ApiVersions answer is the contract, and every client negotiates against it.
func TestGroupAdminAdvertisesTheDesignedWindows(t *testing.T) {
	cl := newClient(t)
	req := kmsg.NewPtrApiVersionsRequest()
	resp, err := req.RequestWith(ctxFor(t, 30*time.Second), cl)
	if err != nil {
		t.Fatalf("ApiVersions: %v", err)
	}
	want := map[int16][2]int16{
		15: {0, 3}, // DescribeGroups: v4 carries group_instance_id
		16: {0, 4}, // ListGroups: v5 carries the KIP-848 group_type
		42: {0, 2}, // DeleteGroups: the whole schema
	}
	seen := map[int16]bool{}
	for _, a := range resp.ApiKeys {
		w, ok := want[a.ApiKey]
		if !ok {
			continue
		}
		seen[a.ApiKey] = true
		if a.MinVersion != w[0] || a.MaxVersion != w[1] {
			t.Errorf("api key %d advertises v%d-v%d, want v%d-v%d",
				a.ApiKey, a.MinVersion, a.MaxVersion, w[0], w[1])
		}
	}
	for key := range want {
		if !seen[key] {
			t.Errorf("api key %d is not advertised at all", key)
		}
	}
}

// CHECK G9b. Whatever a client negotiates, the answers hold: the stopped group
// is Empty, its authorized_operations is the omitted sentinel, and an unknown
// group is GROUP_ID_NOT_FOUND to a delete.
func TestGroupAdminAnswersAtTheNegotiatedVersion(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 2)
	stopped := stoppedGroup(t, topic)
	admin := newClient(t)

	if state := listedState(listGroups(t, admin, listGroupsV), stopped); state != "Empty" {
		t.Errorf("ListGroups states the stopped group as %q, want Empty", state)
	}
	g := described(t, describeGroups(t, admin, describeGroupsV, stopped), stopped)
	if g.ErrorCode != 0 || g.State != "Empty" {
		t.Errorf("DescribeGroups: (%d, %q), want (0, Empty)", g.ErrorCode, g.State)
	}
	if g.AuthorizedOperations != -2147483648 {
		t.Errorf("DescribeGroups answered authorized_operations %d, want the omitted sentinel", g.AuthorizedOperations)
	}
	never := "qk-never-existed-" + newTopic(t)
	if code := deleteCode(t, deleteGroups(t, admin, deleteGroupsV, never), never); code != errGroupIDNotFound {
		t.Errorf("DeleteGroups answered %d for an unknown group, want %d", code, errGroupIDNotFound)
	}
}

// CHECK G10. The group index outlives the FACADE, not only the group's members:
// a facade restart loses the registry and the listing still has the group,
// because existence is in Queen beside the offsets.
//
// It reuses the rig's own restart hook, the same one
// TestGroupOffsetsSurviveAFacadeRestart uses; without QUEEN_KAFKA_RESTART_CMD
// the check skips rather than pretending.
func TestListGroupsSurvivesAFacadeRestart(t *testing.T) {
	topic, _ := seedAcrossPartitions(t, 2)
	stopped := stoppedGroup(t, topic)

	admin := newClient(t)
	if listedState(listGroups(t, admin, listGroupsV), stopped) != "Empty" {
		t.Fatalf("the fixture group %s is not listed before the restart", stopped)
	}

	restart := os.Getenv("QUEEN_KAFKA_RESTART_CMD")
	if restart == "" {
		t.Skip("no QUEEN_KAFKA_RESTART_CMD: run queen-kafka/compat/rig.sh, which sets it")
	}
	out, err := exec.CommandContext(ctxFor(t, 90*time.Second), restart).CombinedOutput()
	if err != nil {
		t.Fatalf("restarting the facade (%s): %v\n%s", restart, err, out)
	}
	// The script names the pid it killed and the one it started, because a
	// restart that silently did nothing would pass the assertion below for the
	// wrong reason.
	pids := strings.TrimSpace(string(out))
	old, fresh := pidField(pids, "old="), pidField(pids, "new=")
	if old == "" || fresh == "" || old == fresh || old == "none" {
		t.Fatalf("the facade was not actually restarted: %q", pids)
	}
	t.Logf("facade restarted: pid %s -> %s", old, fresh)

	after := newClient(t)
	if s := listedState(listGroups(t, after, listGroupsV), stopped); s != "Empty" {
		t.Errorf("after a facade restart %s lists as %q, want Empty", stopped, s)
	}
}
