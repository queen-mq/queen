// flakechk — the acknowledged-push durability checker of PLAN_RAFT.md §13.7,
// for the dropped-unflushed-writes proof owed by R-106 / R-111 / R-114
// (WP-1.11 deliverable 4). Standard library only (the VM has no module cache).
//
// Two modes, driven by flaky-raft1.sh:
//
//   flakechk load   -url U -queue Q -partitions P -count K -batch B -prefix R -ledger F
//       Push K messages (transactionId = "<prefix>-<seq>", payload embeds the
//       seq), over a keep-alive pool. EVERY item the broker answers
//       status="queued" is an ACKNOWLEDGED push (D7: the answer follows the log
//       fsync); its transactionId is appended to the ledger F, and F is fsync'd
//       before load exits. The ledger therefore lists exactly the pushes the
//       broker promised were durable. Prints "acked=<n> pusherr=<e>".
//
//   flakechk verify -url U -queue Q -ledger F
//       Drain Q (wildcard pop, autoAck) until it is empty for -empty-stop polls
//       in a row, collecting the transactionId of every delivered message; then
//       assert EVERY acknowledged push in the ledger is delivered (§13.7).
//       Prints "ledger=<n> delivered=<m> polls=<p> missing=<k>[: ids]".
//       Exit 0 iff nothing acknowledged is missing; 1 on a durability
//       violation; 2 on an operational error.
//
// The ledger lives on the NORMAL disk; only the broker's data dir is on the
// dm-flakey device, so the record of what was acknowledged survives the drop.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: flakechk load|verify -flags")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "load":
		os.Exit(cmdLoad(os.Args[2:]))
	case "verify":
		os.Exit(cmdVerify(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "flakechk: unknown mode %q\n", os.Args[1])
		os.Exit(2)
	}
}

func client() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			MaxConnsPerHost:     256,
		},
	}
}

// ---------------------------------------------------------------- load

func cmdLoad(args []string) int {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:6698", "broker base URL")
	queue := fs.String("queue", "flaky", "queue name")
	partitions := fs.Int("partitions", 8, "partitions to spread over")
	count := fs.Int("count", 2000, "messages to push")
	batch := fs.Int("batch", 20, "messages per push request")
	prefix := fs.String("prefix", "r0", "transactionId prefix (per round)")
	ledger := fs.String("ledger", "", "ledger file for the acked transactionIds (required)")
	concurrency := fs.Int("concurrency", 8, "concurrent pushers")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *ledger == "" {
		fmt.Fprintln(os.Stderr, "flakechk load: -ledger is required")
		return 2
	}
	cl := client()

	type item struct {
		Queue         string `json:"queue"`
		Partition     string `json:"partition"`
		Payload       string `json:"payload"`
		TransactionID string `json:"transactionId"`
	}
	type body struct {
		Items []item `json:"items"`
	}
	type result struct {
		TransactionID string `json:"transaction_id"`
		Status        string `json:"status"`
	}

	jobs := make(chan body, *concurrency*2)
	acks := make(chan string, 4096)
	writerDone := make(chan struct{})
	var pushErr int64

	f, err := os.Create(*ledger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "flakechk load: %v\n", err)
		return 2
	}
	bw := bufio.NewWriter(f)
	acked := 0
	go func() {
		for id := range acks {
			fmt.Fprintln(bw, id)
			acked++
		}
		close(writerDone)
	}()

	pushDone := make(chan struct{}, *concurrency)
	for w := 0; w < *concurrency; w++ {
		go func() {
			for b := range jobs {
				raw, _ := json.Marshal(b)
				req, _ := http.NewRequestWithContext(context.Background(), "POST", *url+"/api/v1/push", bytes.NewReader(raw))
				req.Header.Set("content-type", "application/json")
				resp, err := cl.Do(req)
				if err != nil {
					atomic.AddInt64(&pushErr, int64(len(b.Items)))
					continue
				}
				var results []result
				_ = json.NewDecoder(resp.Body).Decode(&results)
				resp.Body.Close()
				if resp.StatusCode != 201 {
					atomic.AddInt64(&pushErr, int64(len(b.Items)))
					continue
				}
				for _, r := range results {
					if r.Status == "queued" {
						acks <- r.TransactionID
					} else {
						atomic.AddInt64(&pushErr, 1)
					}
				}
			}
			pushDone <- struct{}{}
		}()
	}

	seq := 0
	pad := strings.Repeat("x", 240)
	for seq < *count {
		var b body
		for k := 0; k < *batch && seq < *count; k++ {
			seq++
			b.Items = append(b.Items, item{
				Queue:         *queue,
				Partition:     fmt.Sprintf("p%d", seq%*partitions),
				Payload:       fmt.Sprintf("%s-%d-%s", *prefix, seq, pad),
				TransactionID: fmt.Sprintf("%s-%d", *prefix, seq),
			})
		}
		jobs <- b
	}
	close(jobs)
	for w := 0; w < *concurrency; w++ {
		<-pushDone
	}
	close(acks)
	<-writerDone
	if err := bw.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "flakechk load: ledger flush: %v\n", err)
		return 2
	}
	if err := f.Sync(); err != nil {
		fmt.Fprintf(os.Stderr, "flakechk load: ledger fsync: %v\n", err)
		return 2
	}
	f.Close()
	fmt.Printf("acked=%d pusherr=%d\n", acked, atomic.LoadInt64(&pushErr))
	return 0
}

// ---------------------------------------------------------------- verify

func cmdVerify(args []string) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:6698", "broker base URL")
	queue := fs.String("queue", "flaky", "queue name")
	ledger := fs.String("ledger", "", "ledger file to check against (required)")
	partitions := fs.Int("partitions", 8, "partitions to drain (pinned pop each)")
	emptyStop := fs.Int("empty-stop", 8, "consecutive empty pops per partition that mean it is drained")
	batch := fs.Int("batch", 200, "pop batch")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *ledger == "" {
		fmt.Fprintln(os.Stderr, "flakechk verify: -ledger is required")
		return 2
	}
	cl := client()

	type msg struct {
		TransactionID string `json:"transactionId"`
	}
	type popResp struct {
		Success  bool  `json:"success"`
		Messages []msg `json:"messages"`
	}
	delivered := map[string]bool{}
	polls := 0
	// Drain each partition with a PINNED pop until it is empty for -empty-stop
	// polls in a row. Pinned, per-partition, because at pipeline=1 the wildcard
	// pop lags and the pending gate returns spurious empties, which would stop a
	// single wildcard drain long before the queue is empty. A small backoff on
	// empty lets the pending gate settle.
	for p := 0; p < *partitions; p++ {
		empties := 0
		for empties < *emptyStop {
			polls++
			u := fmt.Sprintf("%s/api/v1/pop/queue/%s/partition/p%d?batch=%d&autoAck=true",
				*url, *queue, p, *batch)
			req, _ := http.NewRequestWithContext(context.Background(), "GET", u, nil)
			resp, err := cl.Do(req)
			if err != nil {
				fmt.Fprintf(os.Stderr, "flakechk verify: pop error: %v\n", err)
				return 2
			}
			var pr popResp
			_ = json.NewDecoder(resp.Body).Decode(&pr)
			resp.Body.Close()
			if len(pr.Messages) == 0 {
				empties++
				time.Sleep(15 * time.Millisecond)
				continue
			}
			empties = 0
			for _, m := range pr.Messages {
				if m.TransactionID != "" {
					delivered[m.TransactionID] = true
				}
			}
		}
	}

	lf, err := os.Open(*ledger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "flakechk verify: %v\n", err)
		return 2
	}
	defer lf.Close()
	ledgerN, missing := 0, 0
	var missingIDs []string
	sc := bufio.NewScanner(lf)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		id := strings.TrimSpace(sc.Text())
		if id == "" {
			continue
		}
		ledgerN++
		if !delivered[id] {
			missing++
			if len(missingIDs) < 10 {
				missingIDs = append(missingIDs, id)
			}
		}
	}
	line := fmt.Sprintf("ledger=%d delivered=%d polls=%d missing=%d", ledgerN, len(delivered), polls, missing)
	if missing > 0 {
		line += ": " + strings.Join(missingIDs, ",")
	}
	fmt.Println(line)
	if missing > 0 {
		return 1
	}
	return 0
}
