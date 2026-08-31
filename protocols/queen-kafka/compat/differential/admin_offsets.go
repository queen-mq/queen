package main

// M7 F4's OffsetDelete scenario (key 47), and the bar it is held to is the ACL
// scenario's: every semantic key here should be IDENTICAL, because this API's
// rules are Kafka's and the facade has the material for all of them. A
// divergence in one of them is a semantics bug, not a deviation.
//
// The rules, measured against apache/kafka:3.9.1 rather than recalled:
//
//   - an unknown group is GROUP_ID_NOT_FOUND at the TOP level, with no
//     partitions in the answer at all;
//   - an EMPTY group has every named partition deletable;
//   - a LIVE consumer group has a partition deletable only if the group is not
//     subscribed to its topic. Subscribed answers GROUP_SUBSCRIBED_TO_TOPIC
//     (86); UNSUBSCRIBED is deleted, live group or not, which is the half of
//     the rule that a merely conservative implementation gets wrong;
//   - deleting an offset that was never committed is 0, not an error.
//
// The ONE deliberate difference is `after.list_groups.contains`, and it was
// measured rather than assumed. On the oracle, deleting the LAST offsets of an
// already-empty group makes the group disappear from ListGroups and answer
// GROUP_ID_NOT_FOUND to the next request; a PARTIAL delete leaves it listed.
// The facade leaves the group listed either way, because OffsetDelete removes
// offsets and DeleteGroups removes groups, and matching the oracle would mean a
// prefix walk on every request to find out whether anything was left. Recorded
// in compat/ERRORS.md and classified in main.go.

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	scenarios = append(scenarios, scenario{
		name: "offsetdelete",
		desc: "deleting a group's committed offsets, with Kafka's subscription rule",
		run:  scenOffsetDelete,
	})
}

// One version, so there is no window to walk.
const offsetDeleteV int16 = 0

// offsetDelete issues one OffsetDelete for one topic's partitions and records
// the top-level code plus one key per partition.
//
// The top-level code is recorded FIRST and always, because it is the field the
// Java AdminClient branches on before it looks at a partition: a group-level
// refusal carries no partitions at all, and a runner that only read partitions
// would record nothing for the case that matters most.
func offsetDelete(c *runctx, prefix string, k *conn, group, topic string, partitions []int32) {
	rt := kmsg.NewOffsetDeleteRequestTopic()
	rt.Topic = topic
	for _, p := range partitions {
		rp := kmsg.NewOffsetDeleteRequestTopicPartition()
		rp.Partition = p
		rt.Partitions = append(rt.Partitions, rp)
	}
	req := kmsg.NewOffsetDeleteRequest()
	req.Group = group
	req.Topics = []kmsg.OffsetDeleteRequestTopic{rt}

	resp, _, err := k.doT(&req, offsetDeleteV, 30*time.Second)
	if err != nil {
		c.rec.bad(prefix, err)
		return
	}
	od := resp.(*kmsg.OffsetDeleteResponse)
	c.rec.add(prefix+".error", "%s", errName(od.ErrorCode))
	c.rec.add(prefix+".n_topics", "%d", len(od.Topics))
	for _, t := range od.Topics {
		for _, p := range t.Partitions {
			c.rec.add(fmt.Sprintf("%s.p%d.error", prefix, p.Partition), "%s", errName(p.ErrorCode))
		}
	}
}

// listedContains renders whether ListGroups still knows about a group, as a
// word rather than a state: the state itself is another scenario's subject and
// the two brokers reap actors on different clocks.
func listedContains(k *conn, group string) string {
	state, err := listGroupState(k, group, nil)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	if state == "<not listed>" {
		return "no"
	}
	return "yes"
}

func scenOffsetDelete(c *runctx) {
	k, err := c.target.dial()
	if err != nil {
		c.rec.bad("dial", err)
		return
	}
	defer k.Close()

	subscribed := c.topic("od-subscribed")
	other := c.topic("od-other")
	for _, t := range []string{subscribed, other} {
		if err := ensureTopic(k, t, c.parts); err != nil {
			c.rec.bad("setup."+t, err)
			return
		}
	}

	// A fresh KRaft broker has no `__consumer_offsets` until something asks for
	// a coordinator, and until it does every group RPC is NOT_COORDINATOR. Rig
	// work, not an observation, and the same loop `admin_groups.go` runs for
	// the same reason.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		req := kmsg.NewFindCoordinatorRequest()
		req.CoordinatorKey = c.group("od-empty")
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

	// ---------------------------------------------------- an EMPTY group
	//
	// A simple-consumer commit (generation -1, no member id) is what makes a
	// group EXIST without making it live, on both brokers. It is also exactly
	// the shape `alterConsumerGroupOffsets` sends.
	empty := c.group("od-empty")
	part, err := offsetCommit(k, empty, -1, "", subscribed, 0, 41, "")
	if err != nil {
		c.rec.bad("empty.commit", err)
		return
	}
	c.rec.add("empty.commit.error", "%s", errName(part.ErrorCode))

	// A partition this group never committed: 0 on both, and the group is not
	// touched by the answer. Sent BEFORE the real delete, because on the oracle
	// the real delete is what makes the group stop existing.
	offsetDelete(c, "never_committed", k, empty, subscribed, []int32{1})
	c.rec.add("never_committed.still_listed", "%s", listedContains(k, empty))

	// ...and now the offset that is there. Every named partition of an empty
	// group is deletable, which is Kafka's first rule.
	offsetDelete(c, "empty_group", k, empty, subscribed, []int32{0})
	c.rec.add("empty_group.committed_after", "%s", committedOffset(k, empty, subscribed, 0))

	// THE deliberate key. The oracle drops a group whose last offset just went
	// away; the facade keeps it listed until DeleteGroups removes it.
	c.rec.add("after.list_groups.contains", "%s", listedContains(k, empty))

	// ----------------------------------------------------- a LIVE group
	//
	// One member, joined and synced, subscribed to `subscribed` and holding a
	// committed offset on `other` as well. That is the shape both halves of
	// Kafka's subscription rule need.
	live := c.group("od-live")
	member, generation, ok := formGroup(c, k, live, subscribed)
	if !ok {
		return
	}
	part, err = offsetCommit(k, live, generation, member, other, 0, 7, "")
	if err != nil {
		c.rec.bad("live.commit.other", err)
		return
	}
	c.rec.add("live.commit.other.error", "%s", errName(part.ErrorCode))

	// Subscribed: refused 86, per partition, and the offset survives.
	offsetDelete(c, "subscribed", k, live, subscribed, []int32{0})
	c.rec.add("subscribed.committed_after", "%s", committedOffset(k, live, subscribed, 0))

	// NOT subscribed, same live group: deleted. This is the key that proves the
	// facade applies Kafka's rule rather than merely refusing whenever a group
	// has members.
	offsetDelete(c, "unsubscribed", k, live, other, []int32{0})
	c.rec.add("unsubscribed.committed_after", "%s", committedOffset(k, live, other, 0))

	// -------------------------------------------------- an UNKNOWN group
	//
	// Top level, and with an empty topics list: the shape
	// `OffsetDeleteRequest.getErrorResponse` builds.
	offsetDelete(c, "unknown_group", k, c.group("od-nobody"), subscribed, []int32{0})
}
