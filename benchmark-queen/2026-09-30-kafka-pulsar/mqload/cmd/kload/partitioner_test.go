package main

import (
	"strconv"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// javaMurmur2 transcribes org.apache.kafka.common.utils.Utils.murmur2.
func javaMurmur2(data []byte) int32 {
	length := int32(len(data))
	const seed uint32 = 0x9747b28c
	const m uint32 = 0x5bd1e995
	const r = 24
	h := seed ^ uint32(length)
	length4 := length / 4
	for i := int32(0); i < length4; i++ {
		i4 := i * 4
		k := uint32(data[i4+0]) + uint32(data[i4+1])<<8 + uint32(data[i4+2])<<16 + uint32(data[i4+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	switch length % 4 {
	case 3:
		h ^= uint32(data[(length&^3)+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[(length&^3)+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[length&^3])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return int32(h)
}

// Java's keyed placement: Utils.toPositive(Utils.murmur2(key)) % numPartitions.
func javaPartition(key []byte, n int) int { return int(javaMurmur2(key)&0x7fffffff) % n }

// E>0 records carry key e<id> and must land where the Java client puts them.
func TestKeyPartitionerMatchesJava(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int32
	}{ // golden vectors of Kafka's UtilsTest.testMurmur2
		{"21", -973932308},
		{"foobar", -790332482},
		{"a-little-bit-long-string", -985981536},
		{"a-little-bit-longer-string", -1486304829},
		{"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971},
		{"abc", 479470107},
	} {
		if got := javaMurmur2([]byte(c.in)); got != c.want {
			t.Fatalf("javaMurmur2(%q) = %d, want %d", c.in, got, c.want)
		}
	}
	tp := kgo.StickyKeyPartitioner(nil).ForTopic("t")
	for _, n := range []int{1, 7, 12, 48, 200, 1000, 100000} {
		for id := 0; id < 200000; id++ {
			key := []byte("e" + strconv.Itoa(id))
			if got, want := tp.Partition(&kgo.Record{Key: key}, n), javaPartition(key, n); got != want {
				t.Fatalf("key %s over %d partitions: franz-go %d, Java %d", key, n, got, want)
			}
		}
	}
}
