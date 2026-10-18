// kfail: does a Kafka client's produce through the Queen facade survive a leader change
// without losing an acknowledged record or reordering one producer's records in a partition?
//
//	kfail produce -brokers a:9092,b:9092 -topic T -partitions 100 -producers 4 -rate 200000 \
//	      -duration 90s -size 1024 -acked acked.bin
//	kfail verify  -brokers a:9092 -topic T -acked acked.bin
//
// produce: idempotent franz-go producers (acks=all, 5 requests in flight per broker, franz-go's
// idempotent default; 1 ms linger; no key: the sticky partitioner, as Kafka's Java client). Every record's value
// starts with its producer number and that producer's sequence number (8 bytes each,
// big-endian); every record the client is told was written is marked in acked.bin.
//
// verify: every partition read from its start to its end. Per (producer, partition) the
// sequence numbers must rise: a number at or below one already read there is a DUPLICATE if
// it was read before anywhere, a REORDER if not. An acknowledged number never read is LOST.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type bitmap []uint64

func (b bitmap) set(i uint64)      { b[i/64] |= 1 << (i % 64) }
func (b bitmap) has(i uint64) bool { return i/64 < uint64(len(b)) && b[i/64]&(1<<(i%64)) != 0 }

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "kfail produce|verify ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "produce":
		produce(os.Args[2:])
	case "verify":
		verify(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "kfail produce|verify ...")
		os.Exit(2)
	}
}

func produce(args []string) {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	brokers := fs.String("brokers", "127.0.0.1:9092", "")
	topic := fs.String("topic", "kfail", "")
	partitions := fs.Int("partitions", 100, "")
	rf := fs.Int("rf", 3, "replication factor of the topic created")
	producers := fs.Int("producers", 4, "")
	rate := fs.Int("rate", 100000, "records per second, all producers")
	duration := fs.Duration("duration", 60*time.Second, "")
	size := fs.Int("size", 1024, "")
	ackedPath := fs.String("acked", "acked.bin", "")
	_ = fs.Parse(args)
	seeds := strings.Split(*brokers, ",")

	adm, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	must(err)
	resp, err := kadm.NewClient(adm).CreateTopics(context.Background(), int32(*partitions), int16(*rf), nil, *topic)
	if err == nil {
		for _, r := range resp {
			fmt.Printf("create %s: %v\n", r.Topic, r.Err)
		}
	} else {
		fmt.Printf("create: %v\n", err)
	}
	adm.Close()

	maxSeq := uint64(*rate / *producers) * uint64(duration.Seconds()+30)
	acked := make([]bitmap, *producers)
	for i := range acked {
		acked[i] = make(bitmap, maxSeq/64+1)
	}
	var mu sync.Mutex
	var sent, ok, failed atomic.Int64
	errs := map[string]int64{}
	var wg sync.WaitGroup
	deadline := time.Now().Add(*duration)
	start := time.Now()
	for p := 0; p < *producers; p++ {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(seeds...),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.ProducerLinger(time.Millisecond),
			kgo.ProducerBatchMaxBytes(1<<20),
			kgo.RecordRetries(1<<30),
			kgo.RecordDeliveryTimeout(5*time.Minute),
			kgo.MaxBufferedRecords(1<<20),
		)
		must(err)
		wg.Add(1)
		go func(p int, cl *kgo.Client) {
			defer wg.Done()
			defer cl.Close()
			perTick := float64(*rate) / float64(*producers) / 1000.0
			var owed float64
			var seq uint64
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for now := range tick.C {
				if now.After(deadline) || seq >= maxSeq {
					break
				}
				owed += perTick
				for owed >= 1 {
					owed--
					v := make([]byte, *size)
					binary.BigEndian.PutUint64(v[0:8], uint64(p))
					binary.BigEndian.PutUint64(v[8:16], seq)
					s := seq
					seq++
					sent.Add(1)
					cl.Produce(context.Background(), &kgo.Record{Topic: *topic, Value: v}, func(_ *kgo.Record, err error) {
						mu.Lock()
						defer mu.Unlock()
						if err != nil {
							failed.Add(1)
							errs[err.Error()]++
							return
						}
						ok.Add(1)
						acked[p].set(s)
					})
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			if err := cl.Flush(ctx); err != nil {
				fmt.Printf("producer %d flush: %v\n", p, err)
			}
		}(p, cl)
	}
	stop := make(chan struct{})
	go func() {
		last := int64(0)
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Second):
				n := ok.Load()
				fmt.Printf("%s acked %d (+%d/s) failed %d sent %d\n", time.Now().UTC().Format("15:04:05"), n, (n-last)/5, failed.Load(), sent.Load())
				last = n
			}
		}
	}()
	wg.Wait()
	close(stop)
	fmt.Printf("done in %s: sent %d acked %d failed %d\n", time.Since(start).Round(time.Second), sent.Load(), ok.Load(), failed.Load())
	for e, n := range errs {
		fmt.Printf("  error %d x %s\n", n, e)
	}
	f, err := os.Create(*ackedPath)
	must(err)
	defer f.Close()
	hdr := make([]byte, 16)
	binary.BigEndian.PutUint64(hdr[0:8], uint64(*producers))
	binary.BigEndian.PutUint64(hdr[8:16], maxSeq)
	_, err = f.Write(hdr)
	must(err)
	buf := make([]byte, 8)
	for _, b := range acked {
		for _, w := range b {
			binary.BigEndian.PutUint64(buf, w)
			_, err = f.Write(buf)
			must(err)
		}
	}
}

func verify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	brokers := fs.String("brokers", "127.0.0.1:9092", "")
	topic := fs.String("topic", "kfail", "")
	ackedPath := fs.String("acked", "acked.bin", "")
	_ = fs.Parse(args)
	seeds := strings.Split(*brokers, ",")

	raw, err := os.ReadFile(*ackedPath)
	must(err)
	producers := int(binary.BigEndian.Uint64(raw[0:8]))
	maxSeq := binary.BigEndian.Uint64(raw[8:16])
	words := int(maxSeq/64 + 1)
	acked := make([]bitmap, producers)
	seen := make([]bitmap, producers)
	off := 16
	for p := 0; p < producers; p++ {
		acked[p] = make(bitmap, words)
		seen[p] = make(bitmap, words)
		for w := 0; w < words; w++ {
			acked[p][w] = binary.BigEndian.Uint64(raw[off : off+8])
			off += 8
		}
	}

	adm, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	must(err)
	ends, err := kadm.NewClient(adm).ListEndOffsets(context.Background(), *topic)
	must(err)
	adm.Close()
	want := map[int32]int64{}
	consume := map[string]map[int32]kgo.Offset{*topic: {}}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && o.Offset > 0 {
			want[o.Partition] = o.Offset
			consume[*topic][o.Partition] = kgo.NewOffset().AtStart()
		}
	})
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.ConsumePartitions(consume), kgo.FetchMaxWait(500*time.Millisecond))
	must(err)
	defer cl.Close()

	type key struct {
		p    uint64
		part int32
	}
	last := map[key]uint64{}
	got := map[int32]int64{}
	var records, dups, reorders, foreign int64
	var reorderEx, dupEx []string
	idle := time.Now()
	for len(got) < len(want) || !allDone(got, want) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		fetches := cl.PollFetches(ctx)
		cancel()
		if fetches.NumRecords() == 0 {
			if time.Since(idle) > 60*time.Second {
				fmt.Println("no records for 60 s; stopping the read short")
				break
			}
			continue
		}
		idle = time.Now()
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				fmt.Printf("fetch %s/%d: %v\n", t, p, err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			records++
			if r.Offset+1 > got[r.Partition] {
				got[r.Partition] = r.Offset + 1
			}
			if len(r.Value) < 16 {
				foreign++
				return
			}
			p := binary.BigEndian.Uint64(r.Value[0:8])
			s := binary.BigEndian.Uint64(r.Value[8:16])
			if p >= uint64(producers) || s >= maxSeq {
				foreign++
				return
			}
			k := key{p, r.Partition}
			prev, any := last[k]
			switch {
			case !any || s > prev:
				last[k] = s
			case seen[p].has(s):
				dups++
				if len(dupEx) < 5 {
					dupEx = append(dupEx, fmt.Sprintf("p%d seq %d again in partition %d at offset %d (after %d)", p, s, r.Partition, r.Offset, prev))
				}
			default:
				reorders++
				if len(reorderEx) < 5 {
					reorderEx = append(reorderEx, fmt.Sprintf("p%d seq %d in partition %d at offset %d, after seq %d", p, s, r.Partition, r.Offset, prev))
				}
			}
			seen[p].set(s)
		})
	}
	var ackedN, lost int64
	var lostEx []string
	for p := 0; p < producers; p++ {
		for s := uint64(0); s < maxSeq; s++ {
			if acked[p].has(s) {
				ackedN++
				if !seen[p].has(s) {
					lost++
					if len(lostEx) < 5 {
						lostEx = append(lostEx, fmt.Sprintf("p%d seq %d", p, s))
					}
				}
			}
		}
	}
	parts := make([]int, 0, len(want))
	for p := range want {
		parts = append(parts, int(p))
	}
	sort.Ints(parts)
	out := map[string]any{
		"partitions": len(parts), "records_read": records, "acked": ackedN, "lost": lost,
		"duplicates": dups, "reorders": reorders, "foreign": foreign,
		"lost_examples": lostEx, "duplicate_examples": dupEx, "reorder_examples": reorderEx,
	}
	j, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(j))
	if lost > 0 || reorders > 0 {
		os.Exit(1)
	}
}

func allDone(got, want map[int32]int64) bool {
	for p, w := range want {
		if got[p] < w {
			return false
		}
	}
	return true
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
