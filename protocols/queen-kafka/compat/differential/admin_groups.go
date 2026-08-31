package main

// M7 F2's oracle scenario: ListGroups, DescribeGroups and DeleteGroups against
// the facade and against a real Apache Kafka, side by side.
//
// It exists because three of this stage's answers are counter-intuitive enough
// that writing them from the protocol documentation would have got them wrong,
// and the runner is cheaper than a wrong handler:
//
//   - a group nobody has ever heard of is DescribeGroups error 0 with state
//     `Dead`, NOT an error;
//   - a group whose last member left is `Empty` and KEEPS its protocol type;
//   - DeleteGroups answers 68 for a group with members and 69 for one that does
//     not exist, and 0 (with the offsets actually gone) in between.
//
// Nothing here records a count of groups on the broker: the oracle accumulates
// every group every scenario made, while the facade lists only this tenant's.
// Every observation is about THIS scenario's own group id.

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	scenarios = append(scenarios, scenario{
		name: "groups-admin",
		desc: "list, describe and delete a consumer group: live, stopped, and never heard of",
		run:  scenGroupsAdmin,
	})
}

const (
	listGroupsV     int16 = 4
	describeGroupsV int16 = 3
	deleteGroupsV   int16 = 2
)

// listOne answers what a ListGroups (optionally filtered by state) says about
// exactly one group id, so the observation is independent of every other group
// on the broker.
func listGroupState(k *conn, group string, states []string) (string, error) {
	req := kmsg.NewListGroupsRequest()
	req.StatesFilter = states
	resp, _, err := k.doT(&req, listGroupsV, 20*time.Second)
	if err != nil {
		return "", err
	}
	lr := resp.(*kmsg.ListGroupsResponse)
	if lr.ErrorCode != 0 {
		return fmt.Sprintf("error=%s", errName(lr.ErrorCode)), nil
	}
	for _, g := range lr.Groups {
		if g.Group == group {
			return fmt.Sprintf("state=%s protocol_type=%q", g.GroupState, g.ProtocolType), nil
		}
	}
	return "<not listed>", nil
}

// describeOne records the whole of one group's description under `prefix`.
func describeGroup(c *runctx, prefix string, k *conn, group string, version int16) {
	req := kmsg.NewDescribeGroupsRequest()
	req.Groups = []string{group}
	req.IncludeAuthorizedOperations = version >= 3
	resp, _, err := k.doT(&req, version, 20*time.Second)
	if err != nil {
		c.rec.bad(prefix, err)
		return
	}
	dr := resp.(*kmsg.DescribeGroupsResponse)
	if len(dr.Groups) != 1 {
		c.rec.bad(prefix, fmt.Errorf("%d results for one group", len(dr.Groups)))
		return
	}
	g := dr.Groups[0]
	c.rec.add(prefix+".error_code", "%s", errName(g.ErrorCode))
	c.rec.add(prefix+".state", "%s", g.State)
	c.rec.add(prefix+".protocol_type", "%s", g.ProtocolType)
	c.rec.add(prefix+".protocol_data", "%s", g.Protocol)
	c.rec.add(prefix+".members", "%d", len(g.Members))
	c.rec.add(prefix+".authorized_operations", "%d", g.AuthorizedOperations)
	if len(g.Members) == 1 {
		m := g.Members[0]
		// The identity fields a broker MINTS are not diffed — a member id and a
		// peer address are the broker's own — but their SHAPE is, because that
		// is what an operator reads.
		c.rec.info(prefix+".member0_id", "%s", m.MemberID)
		c.rec.info(prefix+".member0_host", "%s", m.ClientHost)
		// The client id is the runner's own, and the runner numbers its
		// connections globally — the facade is dialled before Kafka, so the two
		// targets legitimately send different ones. What is diffed is that the
		// broker echoed OURS rather than inventing one.
		c.rec.info(prefix+".member0_client_id", "%s", m.ClientID)
		c.rec.add(prefix+".member0_client_id_is_the_clients", "%t",
			strings.HasPrefix(m.ClientID, "qk-diff-"))
		c.rec.add(prefix+".member0_host_is_an_address", "%t",
			strings.HasPrefix(m.ClientHost, "/") && len(m.ClientHost) > 1)
		c.rec.add(prefix+".member0_instance_id", "%s", showStrPtr(m.InstanceID))
		// The two opaque byte strings, decoded with the client's own reader:
		// what makes them worth passing through is that a client can read them.
		meta := kmsg.NewConsumerMemberMetadata()
		if err := meta.ReadFrom(m.ProtocolMetadata); err != nil {
			c.rec.add(prefix+".member0_metadata", "undecodable: %v", err)
		} else {
			c.rec.add(prefix+".member0_metadata", "topics=%v", meta.Topics)
		}
		assign := kmsg.NewConsumerMemberAssignment()
		if err := assign.ReadFrom(m.MemberAssignment); err != nil {
			c.rec.add(prefix+".member0_assignment", "undecodable: %v", err)
		} else {
			c.rec.add(prefix+".member0_assignment", "%s", showAssignment(m.MemberAssignment))
		}
	}
}

func deleteGroup(k *conn, group string) (int16, error) {
	req := kmsg.NewDeleteGroupsRequest()
	req.Groups = []string{group}
	resp, _, err := k.doT(&req, deleteGroupsV, 20*time.Second)
	if err != nil {
		return 0, err
	}
	dr := resp.(*kmsg.DeleteGroupsResponse)
	if len(dr.Groups) != 1 {
		return 0, fmt.Errorf("%d results for one group", len(dr.Groups))
	}
	if dr.Groups[0].Group != group {
		return 0, fmt.Errorf("the answer names %q", dr.Groups[0].Group)
	}
	return dr.Groups[0].ErrorCode, nil
}

func scenGroupsAdmin(c *runctx) {
	topic := c.topic("gadm")
	group := c.group("admin")
	unknown := c.group("never-existed")

	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()
	if err := ensureTopic(k, topic, c.parts); err != nil {
		c.rec.bad("ensure_topic", err)
		return
	}

	// A fresh KRaft broker has no `__consumer_offsets` until something asks for
	// a coordinator, and until it does every group RPC is NOT_COORDINATOR. Rig
	// work, not an observation.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		req := kmsg.NewFindCoordinatorRequest()
		req.CoordinatorKey = group
		resp, _, err := k.do(&req, 1)
		if err != nil {
			c.rec.bad("coordinator", err)
			return
		}
		if resp.(*kmsg.FindCoordinatorResponse).ErrorCode == 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// ------------------------------------------------ a group nobody has heard of
	//
	// THE observation this scenario was written for. Both at v0 (no
	// `include_authorized_operations` field at all) and at v3.
	describeGroup(c, "unknown.describe_v0", k, unknown, 0)
	describeGroup(c, "unknown.describe_v3", k, unknown, describeGroupsV)
	if listed, err := listGroupState(k, unknown, nil); err != nil {
		c.rec.bad("unknown.list", err)
	} else {
		c.rec.add("unknown.list", "%s", listed)
	}
	if code, err := deleteGroup(k, unknown); err != nil {
		c.rec.bad("unknown.delete", err)
	} else {
		c.rec.add("unknown.delete.error_code", "%s", errName(code))
	}

	// -------------------------------------------------------------- a live group
	member, generation, ok := formGroup(c, k, group, topic)
	if !ok {
		return
	}
	describeGroup(c, "live.describe", k, group, describeGroupsV)
	for _, filter := range [][]string{nil, {"Stable"}, {"Empty"}, {"Nonsense"}} {
		label := "any"
		if len(filter) > 0 {
			label = strings.ToLower(filter[0])
		}
		listed, err := listGroupState(k, group, filter)
		if err != nil {
			c.rec.bad("live.list."+label, err)
			continue
		}
		c.rec.add("live.list."+label, "%s", listed)
	}

	// Kafka's rule: a group with members is not deletable, and nothing is
	// touched. The offsets after the refusal say the second half.
	if code, err := deleteGroup(k, group); err != nil {
		c.rec.bad("live.delete", err)
	} else {
		c.rec.add("live.delete.error_code", "%s", errName(code))
	}
	c.rec.add("live.delete.offsets_survived", "%s", committedOffset(k, group, topic, 0))

	// --------------------------------------------------------- the group empties
	leave := kmsg.NewLeaveGroupRequest()
	leave.Group = group
	leave.MemberID = member
	if _, _, err := k.doT(&leave, leaveV, 20*time.Second); err != nil {
		c.rec.bad("empty.leave", err)
		return
	}
	// Kafka moves a group to Empty on the leave itself; give both a moment
	// rather than racing the FSM.
	time.Sleep(time.Second)
	describeGroup(c, "empty.describe", k, group, describeGroupsV)
	if listed, err := listGroupState(k, group, nil); err != nil {
		c.rec.bad("empty.list", err)
	} else {
		c.rec.add("empty.list", "%s", listed)
	}
	c.rec.add("empty.offsets_survived", "%s", committedOffset(k, group, topic, 0))
	_ = generation

	// ------------------------------------------------------------- and is deleted
	if code, err := deleteGroup(k, group); err != nil {
		c.rec.bad("deleted.delete", err)
	} else {
		c.rec.add("deleted.delete.error_code", "%s", errName(code))
	}
	describeGroup(c, "deleted.describe", k, group, describeGroupsV)
	if listed, err := listGroupState(k, group, nil); err != nil {
		c.rec.bad("deleted.list", err)
	} else {
		c.rec.add("deleted.list", "%s", listed)
	}
	// The whole point of the delete: the committed offset is gone, and "gone"
	// is offset -1 with NO error — which is what makes a consumer apply
	// `auto.offset.reset`.
	c.rec.add("deleted.offset", "%s", committedOffset(k, group, topic, 0))
	// ...and a second delete finds nothing, which is what makes a partially
	// failed delete re-runnable.
	if code, err := deleteGroup(k, group); err != nil {
		c.rec.bad("deleted.delete_again", err)
	} else {
		c.rec.add("deleted.delete_again.error_code", "%s", errName(code))
	}

	// ------------------------------------------------------- names that are not
	for _, bad := range []struct{ label, id string }{
		{"empty_id", ""},
		{"long_id", strings.Repeat("g", 256)},
	} {
		describeGroup(c, "badname."+bad.label+".describe", k, bad.id, describeGroupsV)
		if code, err := deleteGroup(k, bad.id); err != nil {
			c.rec.bad("badname."+bad.label+".delete", err)
		} else {
			c.rec.add("badname."+bad.label+".delete.error_code", "%s", errName(code))
		}
	}
}

// formGroup joins, syncs and commits one member, so the group is Stable with a
// committed offset. Rig work: what it produces is asserted by the observations
// around it, and a failure is recorded as one.
func formGroup(c *runctx, k *conn, group, topic string) (string, int32, bool) {
	first, err := join(k, group, "", topic, 30*time.Second)
	if err != nil {
		c.rec.bad("live.join", err)
		return "", 0, false
	}
	member := first.MemberID
	if member == "" {
		c.rec.bad("live.join", fmt.Errorf("no member id to rejoin with"))
		return "", 0, false
	}
	joined, err := join(k, group, member, topic, 30*time.Second)
	if err != nil {
		c.rec.bad("live.rejoin", err)
		return "", 0, false
	}
	c.rec.add("live.join.error_code", "%s", errName(joined.ErrorCode))
	partitions := make([]int32, c.parts)
	for i := range partitions {
		partitions[i] = int32(i)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	synced, _, err := k.doT(
		syncReq(group, joined.Generation, member, map[string][]int32{member: partitions}, topic),
		syncV, 30*time.Second)
	if err != nil {
		c.rec.bad("live.sync", err)
		return "", 0, false
	}
	c.rec.add("live.sync.error_code", "%s", errName(synced.(*kmsg.SyncGroupResponse).ErrorCode))
	part, err := offsetCommit(k, group, joined.Generation, member, topic, 0, 41, "")
	if err != nil {
		c.rec.bad("live.commit", err)
		return "", 0, false
	}
	c.rec.add("live.commit.error_code", "%s", errName(part.ErrorCode))
	return member, joined.Generation, true
}

// committedOffset reads one partition's committed offset the way an admin tool
// does, and renders it so that "never committed" and "an error" are different
// strings rather than the same -1.
func committedOffset(k *conn, group, topic string, partition int32) string {
	resp, err := offsetFetch(k, group, topic, []int32{partition})
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	if resp.ErrorCode != 0 {
		return fmt.Sprintf("top_level=%s", errName(resp.ErrorCode))
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.Partition != partition {
				continue
			}
			return fmt.Sprintf("offset=%d error=%s", p.Offset, errName(p.ErrorCode))
		}
	}
	return "<not in the answer>"
}
