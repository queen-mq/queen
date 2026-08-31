package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// The SECOND OPINION: segmentio/kafka-go v0.4.51.
//
// franz-go is the strictest client in the matrix and it is what the rest of
// this suite drives, but it is also the one most likely to paper over a
// routing mistake: it has a full metadata cache, a coordinator cache and a
// retry policy that quietly re-runs FindCoordinator on NOT_COORDINATOR.
// kafka-go is the useful contrast for two reasons that are recorded rather
// than assumed:
//
//   - its Conn path writes OffsetCommit v2 and OffsetFetch v1 with no
//     negotiation at all, which is exactly the advertised floor
//     PLAN_QUEEN_KAFKA.md calls load-bearing for it. If NOT_COORDINATOR on
//     that path were mishandled by anyone, it would be here.
//   - its ConsumerGroup does its own coordinator discovery on a plain
//     connection, so "the client followed the redirect" is being asserted of a
//     second, independent implementation of the dance.
//
// The assertion is the same as the franz-go acceptance: two members of one
// group, each bootstrapped against a DIFFERENT facade, deliver every record
// once and share no partition.
func TestKafkaGoTwoMembersAcrossTwoFacades(t *testing.T) {
	if len(nodes) < 2 {
		t.Skip("needs at least two clustered facades")
	}
	const perPartition = 26 // 208 records over 8 partitions
	topic, total := seed(t, addrs(), perPartition)
	group := newName(t, "g")

	owner := assertOneCoordinator(t, group)
	t.Logf("kafka-go: group %s is coordinated by node %d at %s", group, owner.nodeID, owner.addr)

	ledger := newSeen()
	deadline := time.Now().Add(takeoverBudget())
	var wg sync.WaitGroup
	members := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		n := nodes[i]
		member := fmt.Sprintf("kgm%d@%s", i+1, n.addr)
		members = append(members, member)
		r := kafka.NewReader(kafka.ReaderConfig{
			// ONE facade each, and not the whole list: a reader that could
			// bootstrap anywhere might find the coordinator by luck. This one
			// has to be redirected to it.
			Brokers:  []string{n.addr},
			Topic:    topic,
			GroupID:  group,
			MinBytes: 1,
			MaxBytes: 10e6,
			MaxWait:  500 * time.Millisecond,
			// Inside the facade's advertised session window; outside it the
			// answer is INVALID_SESSION_TIMEOUT, which is a config error and
			// not a finding.
			SessionTimeout:    30 * time.Second,
			RebalanceTimeout:  30 * time.Second,
			HeartbeatInterval: 3 * time.Second,
			JoinGroupBackoff:  time.Second,
			StartOffset:       kafka.FirstOffset,
			ReadBatchTimeout:  10 * time.Second,
			// 0 = synchronous commits, so a committed offset is one the facade
			// has actually written rather than one a background timer may yet
			// write.
			CommitInterval: 0,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer r.Close()
			for time.Now().Before(deadline) && ledger.uniqueKeys() < total {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				m, err := r.FetchMessage(ctx)
				cancel()
				if err != nil {
					if time.Now().After(deadline) {
						return
					}
					continue
				}
				ledger.add(member, int32(m.Partition), m.Key)
				cctx, ccancel := context.WithTimeout(context.Background(), 20*time.Second)
				if err := r.CommitMessages(cctx, m); err != nil {
					t.Logf("%s: commit: %v", member, err)
				}
				ccancel()
			}
		}()
	}
	wg.Wait()

	assertOneDelivery(t, ledger, total, members)

	// The offsets kafka-go wrote are readable through every facade, which is
	// the other half of "one coordinator": two coordinators would have written
	// two disjoint halves of this map.
	var sum int64
	for _, n := range nodes {
		got := committed(t, fmt.Sprintf("node-%d", n.id), n.addr, group, topic)
		var s int64
		for _, v := range got {
			if v > 0 {
				s += v
			}
		}
		if sum == 0 {
			sum = s
			continue
		}
		if s != sum {
			t.Errorf("node %d reads kafka-go's committed offsets as %d, node %d reads %d", n.id, s, nodes[0].id, sum)
		}
	}
	if sum != int64(total) {
		t.Errorf("kafka-go's committed offsets sum to %d, want %d", sum, total)
	} else {
		t.Logf("kafka-go committed %d of %d, identical through all %d facades", sum, total, len(nodes))
	}
}
