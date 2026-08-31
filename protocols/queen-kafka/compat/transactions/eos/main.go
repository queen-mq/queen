// compat/transactions/eos -- scenario A6, the decisive one.
//
// Exactly-once consume-transform-produce with franz-go: read from `in`,
// transform, produce to `out`, and commit the input offsets INSIDE the same
// transaction. On one chosen iteration the facade is SIGKILLed between the last
// produce and the commit, and the loop restarts from its committed offsets.
//
// What this proves that no other scenario does: the records and the offsets
// ride ONE POST /api/v1/transaction, which is ONE Postgres transaction
// (DESIGN section 6). So a crash between them is not a window -- the output
// count must equal the input count exactly, with no duplicate and no loss,
// even though a whole iteration's work was thrown away mid-flight.
//
//	env  QK_BOOTSTRAP    host:port of the facade under test
//	     QK_RUN          a suffix that makes every topic and id unique
//	     QK_RESTART_CMD  an executable that SIGKILLs the facade and restarts it
//	     QK_KILL_AT      which iteration to kill on (default 1, 0 disables)
//	     QK_RECORDS      how many input records (default 200)
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	passed int
	failed int
)

func main() {
	bootstrap := env("QK_BOOTSTRAP", "127.0.0.1:32912")
	run := env("QK_RUN", strconv.FormatInt(time.Now().UnixNano(), 10))
	restart := os.Getenv("QK_RESTART_CMD")
	killAt, _ := strconv.Atoi(env("QK_KILL_AT", "1"))
	total, _ := strconv.Atoi(env("QK_RECORDS", "200"))

	in := "qkt-" + run + "-eos-in"
	out := "qkt-" + run + "-eos-out"
	group := "qkt-" + run + "-eos-group"
	txnID := "qkt-" + run + "-eos-txn"

	fmt.Printf("compat/transactions eos  bootstrap=%s  run=%s  kill-at=%d\n", bootstrap, run, killAt)
	if restart == "" {
		fmt.Println("  SKIP  QK_RESTART_CMD is not set; the induced crash is the point of this scenario")
		os.Exit(0)
	}

	if err := seed(bootstrap, in, total); err != nil {
		fmt.Printf("  FAIL  seeding %d input records: %v\n", total, err)
		os.Exit(1)
	}
	fmt.Printf("  seeded %d records into %s\n", total, in)

	killed, produced, err := loop(bootstrap, in, out, group, txnID, total, killAt, restart)
	if err != nil {
		check("the EOS loop ran to completion", false, err.Error())
	} else {
		check("the EOS loop ran to completion", true,
			fmt.Sprintf("%d transactions produced, the facade was killed %d time(s) mid-transaction", produced, killed))
		check("the induced crash actually happened", killed > 0 || killAt == 0,
			fmt.Sprintf("%d kills", killed))
	}

	counts, err := drain(bootstrap, out)
	if err != nil {
		check("the output topic could be read", false, err.Error())
	} else {
		n := 0
		dupes := 0
		for _, c := range counts {
			n += c
			if c > 1 {
				dupes++
			}
		}
		// THE check. A transaction that died between the produce and the
		// commit wrote nothing at all, so its records are reprocessed from the
		// committed offsets and appear exactly once.
		check("every input record is in the output EXACTLY once", n == total && len(counts) == total && dupes == 0,
			fmt.Sprintf("%d records, %d distinct keys, %d duplicated, want %d/%d/0", n, len(counts), dupes, total, total))
		missing := 0
		for i := 0; i < total; i++ {
			if counts[fmt.Sprintf("k%06d", i)] == 0 {
				missing++
			}
		}
		check("no input record was lost", missing == 0, fmt.Sprintf("%d missing", missing))
	}

	fmt.Printf("eos: %d passed, %d failed\n", passed, failed)
	if failed > 0 {
		os.Exit(1)
	}
}

// A plain producer fills the input topic. Not transactional: the input of an
// EOS pipeline is somebody else's output.
func seed(bootstrap, topic string, total int) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("k%06d", i)
		cl.Produce(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte("v" + key)}, nil)
	}
	return cl.Flush(ctx)
}

// The loop. Returns how many times the facade was killed mid-transaction and
// how many transactions committed.
func loop(bootstrap, in, out, group, txnID string, total, killAt int, restart string) (int, int, error) {
	killed := 0
	committedTxns := 0
	iteration := 0
	idle := 0
	seen := 0

	sess, err := session(bootstrap, in, group, txnID)
	if err != nil {
		return killed, committedTxns, err
	}
	defer func() { sess.Close() }()

	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		fetches := sess.PollRecords(ctx, 40)
		cancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			// A fetch error after a restart is expected once: the connection
			// died under the client. It retries on its own.
			for _, e := range errs {
				fmt.Printf("  note  fetch: %v\n", e.Err)
			}
		}
		n := fetches.NumRecords()
		if n == 0 {
			idle++
			if idle >= 3 && seen >= total {
				break
			}
			if idle >= 8 {
				break
			}
			continue
		}
		idle = 0
		iteration++

		if err := sess.Begin(); err != nil {
			return killed, committedTxns, fmt.Errorf("begin on iteration %d: %w", iteration, err)
		}
		pctx, pcancel := context.WithTimeout(context.Background(), 60*time.Second)
		fetches.EachRecord(func(r *kgo.Record) {
			sess.Produce(pctx, &kgo.Record{
				Topic: out,
				Key:   r.Key,
				Value: append([]byte("t:"), r.Value...),
			}, nil)
		})
		if err := sess.Client().Flush(pctx); err != nil {
			fmt.Printf("  note  flush on iteration %d: %v\n", iteration, err)
		}
		pcancel()

		// The induced crash: after the last produce, before the commit. This is
		// the exact instant the design's atomicity claim is about.
		crashed := false
		if iteration == killAt {
			fmt.Printf("  killing the facade between the produce and the commit of iteration %d\n", iteration)
			if err := run(restart); err != nil {
				return killed, committedTxns, fmt.Errorf("restart: %w", err)
			}
			killed++
			crashed = true
		}

		ectx, ecancel := context.WithTimeout(context.Background(), 60*time.Second)
		ok, err := sess.End(ectx, kgo.TryCommit)
		ecancel()
		if err != nil || !ok {
			if !crashed {
				return killed, committedTxns, fmt.Errorf("End on iteration %d: committed=%v err=%w", iteration, ok, err)
			}
			// Expected: the stage died with the facade, so EndTxn is answered
			// INVALID_TXN_STATE, which is fatal for the client. A restarted
			// application is exactly what this models, so the session is
			// rebuilt and the loop resumes from its COMMITTED offsets.
			fmt.Printf("  note  the commit after the induced crash failed as designed: committed=%v err=%v\n", ok, err)
			sess.Close()
			seen = 0
			sess, err = session(bootstrap, in, group, txnID)
			if err != nil {
				return killed, committedTxns, fmt.Errorf("rebuilding the session: %w", err)
			}
			continue
		}
		committedTxns++
		seen += n
	}
	return killed, committedTxns, nil
}

func session(bootstrap, in, group, txnID string) (*kgo.GroupTransactSession, error) {
	return kgo.NewGroupTransactSession(
		kgo.SeedBrokers(bootstrap),
		kgo.TransactionalID(txnID),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(in),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AllowAutoTopicCreation(),
		kgo.TransactionTimeout(2*time.Minute),
	)
}

// Read a topic whole and count occurrences per key, which is what turns "the
// counts are equal" into "no duplicate and no loss".
func drain(bootstrap, topic string) (map[string]int, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	counts := map[string]int{}
	idle := 0
	for idle < 3 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		fetches := cl.PollRecords(ctx, 500)
		cancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != context.DeadlineExceeded {
					fmt.Printf("  note  drain: %v\n", e.Err)
				}
			}
		}
		if fetches.NumRecords() == 0 {
			idle++
			continue
		}
		idle = 0
		fetches.EachRecord(func(r *kgo.Record) { counts[string(r.Key)]++ })
	}
	return counts, nil
}

func run(cmd string) error {
	c := exec.Command(cmd)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}

func check(what string, ok bool, detail string) {
	if ok {
		passed++
		fmt.Printf("  PASS  %s  [%s]\n", what, detail)
		return
	}
	failed++
	fmt.Printf("  FAIL  %s  [%s]\n", what, detail)
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
