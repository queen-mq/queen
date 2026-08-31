package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	scenarios = append(scenarios, scenario{
		name: "group",
		desc: "the join/sync/heartbeat/commit/leave dance at the wire level, two members, no client library helping",
		run:  scenGroup,
	})
}

const (
	joinV      int16 = 4
	syncV      int16 = 2
	heartbeatV int16 = 2
	leaveV     int16 = 2
	commitV    int16 = 6
	ofetchV    int16 = 5
)

func joinReq(group, memberID, topic string) *kmsg.JoinGroupRequest {
	req := kmsg.NewJoinGroupRequest()
	req.Group = group
	req.SessionTimeoutMillis = 10_000
	req.RebalanceTimeoutMillis = 15_000
	req.MemberID = memberID
	req.ProtocolType = "consumer"
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = []string{topic}
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name = "range"
	p.Metadata = meta.AppendTo(nil)
	req.Protocols = []kmsg.JoinGroupRequestProtocol{p}
	return &req
}

func join(k *conn, group, memberID, topic string, timeout time.Duration) (*kmsg.JoinGroupResponse, error) {
	resp, _, err := k.doT(joinReq(group, memberID, topic), joinV, timeout)
	if err != nil {
		return nil, err
	}
	return resp.(*kmsg.JoinGroupResponse), nil
}

func syncReq(group string, generation int32, memberID string, assign map[string][]int32, topic string) *kmsg.SyncGroupRequest {
	req := kmsg.NewSyncGroupRequest()
	req.Group = group
	req.Generation = generation
	req.MemberID = memberID
	members := make([]string, 0, len(assign))
	for m := range assign {
		members = append(members, m)
	}
	sort.Strings(members)
	for _, m := range members {
		a := kmsg.NewConsumerMemberAssignment()
		a.Version = 0
		at := kmsg.NewConsumerMemberAssignmentTopic()
		at.Topic = topic
		at.Partitions = assign[m]
		a.Topics = []kmsg.ConsumerMemberAssignmentTopic{at}
		ga := kmsg.NewSyncGroupRequestGroupAssignment()
		ga.MemberID = m
		ga.MemberAssignment = a.AppendTo(nil)
		req.GroupAssignment = append(req.GroupAssignment, ga)
	}
	return &req
}

func heartbeat(k *conn, group string, generation int32, memberID string) (*kmsg.HeartbeatResponse, error) {
	req := kmsg.NewHeartbeatRequest()
	req.Group = group
	req.Generation = generation
	req.MemberID = memberID
	resp, _, err := k.doT(&req, heartbeatV, 20*time.Second)
	if err != nil {
		return nil, err
	}
	return resp.(*kmsg.HeartbeatResponse), nil
}

func offsetCommit(k *conn, group string, generation int32, memberID, topic string, partition int32, offset int64, meta string) (*kmsg.OffsetCommitResponseTopicPartition, error) {
	req := kmsg.NewOffsetCommitRequest()
	req.Group = group
	req.Generation = generation
	req.MemberID = memberID
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic = topic
	cp := kmsg.NewOffsetCommitRequestTopicPartition()
	cp.Partition = partition
	cp.Offset = offset
	cp.LeaderEpoch = -1
	cp.Metadata = &meta
	ct.Partitions = []kmsg.OffsetCommitRequestTopicPartition{cp}
	req.Topics = []kmsg.OffsetCommitRequestTopic{ct}
	resp, _, err := k.doT(&req, commitV, 20*time.Second)
	if err != nil {
		return nil, err
	}
	cr := resp.(*kmsg.OffsetCommitResponse)
	if len(cr.Topics) != 1 || len(cr.Topics[0].Partitions) != 1 {
		return nil, fmt.Errorf("an OffsetCommit response with %d topics", len(cr.Topics))
	}
	return &cr.Topics[0].Partitions[0], nil
}

func offsetFetch(k *conn, group, topic string, partitions []int32) (*kmsg.OffsetFetchResponse, error) {
	req := kmsg.NewOffsetFetchRequest()
	req.Group = group
	ft := kmsg.NewOffsetFetchRequestTopic()
	ft.Topic = topic
	ft.Partitions = partitions
	req.Topics = []kmsg.OffsetFetchRequestTopic{ft}
	resp, _, err := k.doT(&req, ofetchV, 20*time.Second)
	if err != nil {
		return nil, err
	}
	return resp.(*kmsg.OffsetFetchResponse), nil
}

func showAssignment(raw []byte) string {
	if len(raw) == 0 {
		return "<empty>"
	}
	a := kmsg.NewConsumerMemberAssignment()
	if err := a.ReadFrom(raw); err != nil {
		return fmt.Sprintf("<undecodable: %v>", err)
	}
	var parts []string
	for _, t := range a.Topics {
		var ps []string
		for _, p := range t.Partitions {
			ps = append(ps, fmt.Sprint(p))
		}
		parts = append(parts, fmt.Sprintf("%s[%s]", "<topic>", strings.Join(ps, " ")))
	}
	if len(parts) == 0 {
		return "version=" + fmt.Sprint(a.Version) + " no topics"
	}
	return strings.Join(parts, ",")
}

func scenGroup(c *runctx) {
	topic := c.topic("grp")
	group := c.group("main")

	setup, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial.setup", err)
		return
	}
	defer setup.Close()
	if err := ensureTopic(setup, topic, c.parts); err != nil {
		c.rec.bad("ensure_topic", err)
		return
	}

	// ------------------------------------------------------- coordinator
	var coordErr int16 = -1
	attempts := 0
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		req := kmsg.NewFindCoordinatorRequest()
		req.CoordinatorKey = group
		req.CoordinatorType = 0
		resp, _, err := setup.do(&req, 1)
		if err != nil {
			c.rec.bad("coordinator", err)
			return
		}
		fc := resp.(*kmsg.FindCoordinatorResponse)
		attempts++
		coordErr = fc.ErrorCode
		if coordErr == 0 {
			c.rec.info("coordinator.node", "id=%d host=%s port=%d", fc.NodeID, fc.Host, fc.Port)
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.rec.add("coordinator.error_code", "%s", errName(coordErr))
	c.rec.info("coordinator.attempts", "%d", attempts)
	if coordErr != 0 {
		return
	}

	a, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial.a", err)
		return
	}
	defer a.Close()
	b, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial.b", err)
		return
	}
	defer b.Close()

	// ------------------------------------ join with an empty member id (v4)
	j1, err := join(a, group, "", topic, 30*time.Second)
	if err != nil {
		c.rec.bad("join.empty_member_id", err)
		return
	}
	c.rec.add("join.empty_member_id.error_code", "%s", errName(j1.ErrorCode))
	c.rec.add("join.empty_member_id.member_id_issued", "%t", j1.MemberID != "")
	c.rec.add("join.empty_member_id.generation", "%d", j1.Generation)
	c.rec.add("join.empty_member_id.protocol", "%s", showStrPtr(j1.Protocol))
	c.rec.add("join.empty_member_id.leader_is_empty", "%t", j1.LeaderID == "")
	c.rec.add("join.empty_member_id.members", "%d", len(j1.Members))
	memberA := j1.MemberID
	if memberA == "" {
		c.rec.bad("join.empty_member_id", fmt.Errorf("no member id to rejoin with"))
		return
	}

	// ------------------------------------------- the rejoin that succeeds
	j2, err := join(a, group, memberA, topic, 30*time.Second)
	if err != nil {
		c.rec.bad("join.rejoin", err)
		return
	}
	c.rec.add("join.rejoin.error_code", "%s", errName(j2.ErrorCode))
	c.rec.add("join.rejoin.generation", "%d", j2.Generation)
	c.rec.add("join.rejoin.member_id_echoed", "%t", j2.MemberID == memberA)
	c.rec.add("join.rejoin.is_leader", "%t", j2.LeaderID == memberA)
	c.rec.add("join.rejoin.members", "%d", len(j2.Members))
	c.rec.add("join.rejoin.protocol", "%s", showStrPtr(j2.Protocol))
	c.rec.add("join.rejoin.protocol_type", "%s", showStrPtr(j2.ProtocolType))
	if len(j2.Members) == 1 {
		c.rec.add("join.rejoin.member0_is_self", "%t", j2.Members[0].MemberID == memberA)
		c.rec.add("join.rejoin.member0_metadata_len", "%d", len(j2.Members[0].ProtocolMetadata))
	}
	gen1 := j2.Generation

	// A one-member group can sync straight away: that is the state the
	// second member has to disturb.
	s1, _, err := a.doT(syncReq(group, gen1, memberA, map[string][]int32{memberA: {0, 1, 2, 3, 4, 5, 6, 7}}, topic), syncV, 30*time.Second)
	if err != nil {
		c.rec.bad("sync.solo", err)
		return
	}
	sr1 := s1.(*kmsg.SyncGroupResponse)
	c.rec.add("sync.solo.error_code", "%s", errName(sr1.ErrorCode))
	c.rec.add("sync.solo.assignment", "%s", showAssignment(sr1.MemberAssignment))

	if hb, err := heartbeat(a, group, gen1, memberA); err != nil {
		c.rec.bad("heartbeat.stable", err)
	} else {
		c.rec.add("heartbeat.stable.error_code", "%s", errName(hb.ErrorCode))
	}

	// ------------------------------------------------- a second member joins
	jb1, err := join(b, group, "", topic, 30*time.Second)
	if err != nil {
		c.rec.bad("join.b.empty_member_id", err)
		return
	}
	c.rec.add("join.b.empty_member_id.error_code", "%s", errName(jb1.ErrorCode))
	c.rec.add("join.b.empty_member_id.member_id_issued", "%t", jb1.MemberID != "")
	memberB := jb1.MemberID
	if memberB == "" {
		c.rec.bad("join.b.empty_member_id", fmt.Errorf("no member id to rejoin with"))
		return
	}
	c.rec.add("join.b.member_id_differs_from_a", "%t", memberB != memberA)

	pendB, err := b.begin(joinReq(group, memberB, topic), joinV)
	if err != nil {
		c.rec.bad("join.b.begin", err)
		return
	}
	time.Sleep(700 * time.Millisecond)

	// While the group is rebalancing, the member that has not rejoined is
	// told so.
	if hb, err := heartbeat(a, group, gen1, memberA); err != nil {
		c.rec.bad("heartbeat.during_rebalance", err)
	} else {
		c.rec.add("heartbeat.during_rebalance.error_code", "%s", errName(hb.ErrorCode))
	}

	answered, err := pendB.answered(300 * time.Millisecond)
	if err != nil {
		c.rec.bad("join.b.pending_probe", err)
	} else {
		c.rec.add("join.b.answered_before_leader_rejoined", "%t", answered)
	}

	ja3, err := join(a, group, memberA, topic, 40*time.Second)
	if err != nil {
		c.rec.bad("join.a.rebalance", err)
		return
	}
	c.rec.add("join.a.rebalance.error_code", "%s", errName(ja3.ErrorCode))
	c.rec.add("join.a.rebalance.generation_is_previous_plus_1", "%t", ja3.Generation == gen1+1)
	c.rec.add("join.a.rebalance.generation", "%d", ja3.Generation)
	c.rec.add("join.a.rebalance.members", "%d", len(ja3.Members))
	c.rec.add("join.a.rebalance.is_leader", "%t", ja3.LeaderID == memberA)
	gen2 := ja3.Generation

	rb, _, err := pendB.wait(40 * time.Second)
	if err != nil {
		c.rec.bad("join.b.rebalance", err)
		return
	}
	jb2 := rb.(*kmsg.JoinGroupResponse)
	c.rec.add("join.b.rebalance.error_code", "%s", errName(jb2.ErrorCode))
	c.rec.add("join.b.rebalance.generation_matches_leader", "%t", jb2.Generation == gen2)
	c.rec.add("join.b.rebalance.is_follower", "%t", jb2.LeaderID == memberA && jb2.MemberID == memberB)
	c.rec.add("join.b.rebalance.members", "%d", len(jb2.Members))
	c.rec.add("join.b.rebalance.protocol", "%s", showStrPtr(jb2.Protocol))

	// ------------------------------- the follower syncs before the leader
	pendSyncB, err := b.begin(syncReq(group, gen2, memberB, nil, topic), syncV)
	if err != nil {
		c.rec.bad("sync.b.begin", err)
		return
	}
	time.Sleep(1 * time.Second)
	early, err := pendSyncB.answered(300 * time.Millisecond)
	if err != nil {
		c.rec.bad("sync.b.pending_probe", err)
	} else {
		c.rec.add("sync.follower.answered_before_leader_synced", "%t", early)
	}

	plan := map[string][]int32{memberA: {0, 1, 2, 3}, memberB: {4, 5, 6, 7}}
	sa, _, err := a.doT(syncReq(group, gen2, memberA, plan, topic), syncV, 40*time.Second)
	if err != nil {
		c.rec.bad("sync.leader", err)
		return
	}
	sra := sa.(*kmsg.SyncGroupResponse)
	c.rec.add("sync.leader.error_code", "%s", errName(sra.ErrorCode))
	c.rec.add("sync.leader.assignment", "%s", showAssignment(sra.MemberAssignment))

	sb, _, err := pendSyncB.wait(40 * time.Second)
	if err != nil {
		c.rec.bad("sync.follower", err)
		return
	}
	srb := sb.(*kmsg.SyncGroupResponse)
	c.rec.add("sync.follower.error_code", "%s", errName(srb.ErrorCode))
	c.rec.add("sync.follower.assignment", "%s", showAssignment(srb.MemberAssignment))

	// ---------------------------------------------------------- heartbeats
	if hb, err := heartbeat(a, group, gen1, memberA); err != nil {
		c.rec.bad("heartbeat.stale_generation", err)
	} else {
		c.rec.add("heartbeat.stale_generation.error_code", "%s", errName(hb.ErrorCode))
	}
	if hb, err := heartbeat(a, group, gen2+5, memberA); err != nil {
		c.rec.bad("heartbeat.future_generation", err)
	} else {
		c.rec.add("heartbeat.future_generation.error_code", "%s", errName(hb.ErrorCode))
	}
	if hb, err := heartbeat(a, group, gen2, "not-a-member-of-this-group"); err != nil {
		c.rec.bad("heartbeat.unknown_member", err)
	} else {
		c.rec.add("heartbeat.unknown_member.error_code", "%s", errName(hb.ErrorCode))
	}
	if hb, err := heartbeat(a, group, gen2, memberA); err != nil {
		c.rec.bad("heartbeat.current", err)
	} else {
		c.rec.add("heartbeat.current.error_code", "%s", errName(hb.ErrorCode))
	}

	// -------------------------------------------------------- offset commit
	if oc, err := offsetCommit(a, group, gen2+5, memberA, topic, 0, 42, "m"); err != nil {
		c.rec.bad("commit.wrong_generation", err)
	} else {
		c.rec.add("commit.wrong_generation.error_code", "%s", errName(oc.ErrorCode))
	}
	if oc, err := offsetCommit(a, group, gen1, memberA, topic, 0, 42, "m"); err != nil {
		c.rec.bad("commit.stale_generation", err)
	} else {
		c.rec.add("commit.stale_generation.error_code", "%s", errName(oc.ErrorCode))
	}
	if oc, err := offsetCommit(a, group, gen2, "not-a-member-of-this-group", topic, 0, 42, "m"); err != nil {
		c.rec.bad("commit.unknown_member", err)
	} else {
		c.rec.add("commit.unknown_member.error_code", "%s", errName(oc.ErrorCode))
	}
	if oc, err := offsetCommit(a, group, gen2, memberA, topic, 0, 42, "committed-by-the-differential-runner"); err != nil {
		c.rec.bad("commit.ok", err)
	} else {
		c.rec.add("commit.ok.error_code", "%s", errName(oc.ErrorCode))
	}

	// After a wrong-generation commit was refused, nothing may have been
	// written: the read-back is the only proof.
	if of, err := offsetFetch(a, group, topic, []int32{0, 1}); err != nil {
		c.rec.bad("offsetfetch.committed", err)
	} else {
		c.rec.add("offsetfetch.committed.error_code", "%s", errName(of.ErrorCode))
		byPart := map[int32]kmsg.OffsetFetchResponseTopicPartition{}
		for _, t := range of.Topics {
			for _, p := range t.Partitions {
				byPart[p.Partition] = p
			}
		}
		c.rec.add("offsetfetch.committed.topics", "%d", len(of.Topics))
		if p, ok := byPart[0]; ok {
			c.rec.add("offsetfetch.committed.p0.offset", "%d", p.Offset)
			c.rec.add("offsetfetch.committed.p0.error_code", "%s", errName(p.ErrorCode))
			c.rec.add("offsetfetch.committed.p0.metadata", "%s", showStrPtr(p.Metadata))
			c.rec.add("offsetfetch.committed.p0.leader_epoch", "%d", p.LeaderEpoch)
		} else {
			c.rec.add("offsetfetch.committed.p0.offset", "<partition absent from the response>")
		}
		if p, ok := byPart[1]; ok {
			c.rec.add("offsetfetch.committed.p1.offset", "%d", p.Offset)
			c.rec.add("offsetfetch.committed.p1.error_code", "%s", errName(p.ErrorCode))
			c.rec.add("offsetfetch.committed.p1.metadata", "%s", showStrPtr(p.Metadata))
		} else {
			c.rec.add("offsetfetch.committed.p1.offset", "<partition absent from the response>")
		}
	}

	// A group that has never committed anything.
	virgin := c.group("never-touched")
	if of, err := offsetFetch(a, virgin, topic, []int32{0}); err != nil {
		c.rec.bad("offsetfetch.uncommitted", err)
	} else {
		c.rec.add("offsetfetch.uncommitted.error_code", "%s", errName(of.ErrorCode))
		if len(of.Topics) == 1 && len(of.Topics[0].Partitions) == 1 {
			p := of.Topics[0].Partitions[0]
			c.rec.add("offsetfetch.uncommitted.offset", "%d", p.Offset)
			c.rec.add("offsetfetch.uncommitted.p0.error_code", "%s", errName(p.ErrorCode))
			c.rec.add("offsetfetch.uncommitted.metadata", "%s", showStrPtr(p.Metadata))
			c.rec.add("offsetfetch.uncommitted.leader_epoch", "%d", p.LeaderEpoch)
		} else {
			c.rec.add("offsetfetch.uncommitted.offset", "<no such topic/partition in the response>")
		}
	}

	// ------------------------------------------------ leave, then come back
	lreq := kmsg.NewLeaveGroupRequest()
	lreq.Group = group
	lreq.MemberID = memberB
	if lr, _, err := b.doT(&lreq, leaveV, 20*time.Second); err != nil {
		c.rec.bad("leave", err)
	} else {
		c.rec.add("leave.error_code", "%s", errName(lr.(*kmsg.LeaveGroupResponse).ErrorCode))
	}
	// Leaving is a rebalance for whoever stayed.
	if hb, err := heartbeat(a, group, gen2, memberA); err != nil {
		c.rec.bad("heartbeat.after_leave", err)
	} else {
		c.rec.add("heartbeat.after_leave.error_code", "%s", errName(hb.ErrorCode))
	}
	// The member that left is unknown again.
	if hb, err := heartbeat(b, group, gen2, memberB); err != nil {
		c.rec.bad("heartbeat.by_departed_member", err)
	} else {
		c.rec.add("heartbeat.by_departed_member.error_code", "%s", errName(hb.ErrorCode))
	}
	// Leaving twice.
	lreq2 := kmsg.NewLeaveGroupRequest()
	lreq2.Group = group
	lreq2.MemberID = memberB
	if lr, _, err := b.doT(&lreq2, leaveV, 20*time.Second); err != nil {
		c.rec.bad("leave.twice", err)
	} else {
		c.rec.add("leave.twice.error_code", "%s", errName(lr.(*kmsg.LeaveGroupResponse).ErrorCode))
	}

	jb3, err := join(b, group, "", topic, 30*time.Second)
	if err != nil {
		c.rec.bad("rejoin_after_leave.empty_member_id", err)
		return
	}
	c.rec.add("rejoin_after_leave.empty_member_id.error_code", "%s", errName(jb3.ErrorCode))
	c.rec.add("rejoin_after_leave.member_id_is_new", "%t", jb3.MemberID != memberB && jb3.MemberID != "")
	memberB2 := jb3.MemberID
	if memberB2 == "" {
		return
	}

	pendB2, err := b.begin(joinReq(group, memberB2, topic), joinV)
	if err != nil {
		c.rec.bad("rejoin_after_leave.begin", err)
		return
	}
	time.Sleep(500 * time.Millisecond)
	ja4, err := join(a, group, memberA, topic, 40*time.Second)
	if err != nil {
		c.rec.bad("rejoin_after_leave.a", err)
		return
	}
	c.rec.add("rejoin_after_leave.a.error_code", "%s", errName(ja4.ErrorCode))
	c.rec.add("rejoin_after_leave.a.generation_is_previous_plus_1", "%t", ja4.Generation == gen2+1)
	c.rec.add("rejoin_after_leave.a.members", "%d", len(ja4.Members))
	rb2, _, err := pendB2.wait(40 * time.Second)
	if err != nil {
		c.rec.bad("rejoin_after_leave.b", err)
		return
	}
	jb4 := rb2.(*kmsg.JoinGroupResponse)
	c.rec.add("rejoin_after_leave.b.error_code", "%s", errName(jb4.ErrorCode))
	c.rec.add("rejoin_after_leave.b.generation_matches_leader", "%t", jb4.Generation == ja4.Generation)

	// The offsets committed under an older generation survive a rebalance.
	if of, err := offsetFetch(a, group, topic, []int32{0}); err != nil {
		c.rec.bad("offsetfetch.after_rebalance", err)
	} else if len(of.Topics) == 1 && len(of.Topics[0].Partitions) == 1 {
		c.rec.add("offsetfetch.after_rebalance.offset", "%d", of.Topics[0].Partitions[0].Offset)
		c.rec.add("offsetfetch.after_rebalance.metadata", "%s", showStrPtr(of.Topics[0].Partitions[0].Metadata))
	} else {
		c.rec.add("offsetfetch.after_rebalance.offset", "<no such topic/partition in the response>")
	}
}
