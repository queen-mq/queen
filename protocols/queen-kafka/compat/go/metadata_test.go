package compat

import (
	"strconv"
	"strings"
	"testing"
)

// CHECK 1. What a client learns before it does anything else: one broker, at
// the address the facade was told to advertise, owning every partition of a
// topic that did not exist a moment ago.
func TestMetadataAdvertisesOneBrokerAndTheAutoCreatedTopic(t *testing.T) {
	cl := newClient(t)
	topic := newTopic(t)
	md := metadataFor(t, cl, topic)

	if len(md.Brokers) != 1 {
		t.Fatalf("metadata lists %d brokers, want exactly 1: %+v", len(md.Brokers), md.Brokers)
	}
	b := md.Brokers[0]
	wantHost, wantPort := splitHostPort(t, bootstrap())
	if b.Host != wantHost || b.Port != wantPort {
		t.Errorf("advertised broker is %s:%d, want %s:%d — a client would reconnect to the wrong address",
			b.Host, b.Port, wantHost, wantPort)
	}
	if md.ControllerID != b.NodeID {
		t.Errorf("controller id %d is not the only broker's node id %d", md.ControllerID, b.NodeID)
	}

	var found *string
	width := topicWidth(t)
	for i := range md.Topics {
		mt := &md.Topics[i]
		if mt.Topic == nil || *mt.Topic != topic {
			continue
		}
		found = mt.Topic
		if mt.ErrorCode != 0 {
			t.Fatalf("topic %s came back with error code %d", topic, mt.ErrorCode)
		}
		if int32(len(mt.Partitions)) != width {
			t.Fatalf("topic %s has %d partitions, want QUEEN_KAFKA_DEFAULT_PARTITIONS=%d",
				topic, len(mt.Partitions), width)
		}
		seen := make(map[int32]bool, width)
		for _, p := range mt.Partitions {
			if p.ErrorCode != 0 {
				t.Errorf("partition %d: error code %d", p.Partition, p.ErrorCode)
			}
			if seen[p.Partition] {
				t.Errorf("partition %d appears twice", p.Partition)
			}
			seen[p.Partition] = true
			if p.Partition < 0 || p.Partition >= width {
				t.Errorf("partition index %d is outside 0..%d", p.Partition, width-1)
			}
			// One logical broker: it is the leader of everything, the only
			// replica and the whole ISR.
			if p.Leader != b.NodeID {
				t.Errorf("partition %d leader is %d, want %d", p.Partition, p.Leader, b.NodeID)
			}
			if len(p.Replicas) != 1 || p.Replicas[0] != b.NodeID {
				t.Errorf("partition %d replicas %v, want [%d]", p.Partition, p.Replicas, b.NodeID)
			}
			if len(p.ISR) != 1 || p.ISR[0] != b.NodeID {
				t.Errorf("partition %d ISR %v, want [%d]", p.Partition, p.ISR, b.NodeID)
			}
			if len(p.OfflineReplicas) != 0 {
				t.Errorf("partition %d has offline replicas %v", p.Partition, p.OfflineReplicas)
			}
		}
	}
	if found == nil {
		t.Fatalf("topic %s is missing from the metadata response", topic)
	}

	// A second look is served from the same catalog and must not have grown a
	// second copy of the queue.
	again := metadataFor(t, cl, topic)
	n := 0
	for _, mt := range again.Topics {
		if mt.Topic != nil && *mt.Topic == topic {
			n++
		}
	}
	if n != 1 {
		t.Errorf("the topic appears %d times on a re-read, want once", n)
	}
}

func splitHostPort(t *testing.T, addr string) (string, int32) {
	t.Helper()
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		t.Fatalf("bootstrap address %q has no port", addr)
	}
	port, err := strconv.Atoi(addr[i+1:])
	if err != nil {
		t.Fatalf("bootstrap address %q has no usable port: %v", addr, err)
	}
	return addr[:i], int32(port)
}
