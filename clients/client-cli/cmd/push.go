package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	queen "github.com/smartpricing/queen/client-go"
	"github.com/spf13/cobra"
)

var (
	pushPartition    string
	pushPartitionKey string
	pushBatch        int
	pushData         string
	pushFile         string
	pushTrace        string
	pushTxID         string
	pushWrapped      bool
	pushDryRun       bool
)

var pushCmd = &cobra.Command{
	Use:   "push <queue>",
	Short: "Push messages to a queue (NDJSON on stdin or --data)",
	Long: `Push one or many messages to a queue. By default, reads NDJSON
from stdin (one JSON object per line) and pushes in batches.

  echo '{"hello":"world"}' | queenctl push orders
  cat events.ndjson | queenctl push events --batch 500 --partition-key user_id
  queenctl push orders --data '{"id":1}'
  queenctl push orders --file events.ndjson

If --partition-key is set, the value at that JSON path is used as the
partition for each message (one ordered lane per key).

By default each input line is treated as the message data (payload). With
--wrapped the broker's item shape is expected instead, so each line is a
JSON object with {transactionId?, traceId?, partition?, data}. Useful for
producing across queues with explicit transaction ids and for replicating
between brokers.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		queueName := args[0]
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		ctx := cmd.Context()
		var src io.Reader
		switch {
		case pushData != "":
			src = strings.NewReader(pushData + "\n")
		case pushFile != "":
			f, err := os.Open(pushFile)
			if err != nil {
				return clierr.Userf("open %s: %v", pushFile, err)
			}
			defer f.Close()
			src = f
		default:
			src = os.Stdin
		}

		batch := pushBatch
		if batch <= 0 {
			batch = 100
		}

		queued, dup, failed, err := pushNDJSON(ctx, c.Q, queueName, src, batch)
		if err != nil {
			return err
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "queued=%d duplicate=%d failed=%d\n", queued, dup, failed)
		}
		// Empty (exit 4) only when there was nothing to push at all - i.e.
		// stdin was empty / file had no records. Duplicate is a successful
		// idempotent outcome and must not be reported as Empty.
		if queued == 0 && failed == 0 && dup == 0 {
			return clierr.Empty("nothing pushed")
		}
		return nil
	},
}

// pushItem is the internal staging shape used while buffering NDJSON before
// a single batched POST /api/v1/push.
type pushItem struct {
	queue         string
	partition     string
	payload       any
	transactionID string
	traceID       string
}

// wrappedItem matches the SDK's PushItem JSON wire shape, used when --wrapped
// is set so callers can supply explicit transactionId / traceId / partition
// per message:
//
//	{"transactionId":"...","traceId":"...","partition":"p0","data":{...}}
type wrappedItem struct {
	TransactionID string `json:"transactionId,omitempty"`
	TraceID       string `json:"traceId,omitempty"`
	Partition     string `json:"partition,omitempty"`
	Data          any    `json:"data"`
}

func pushNDJSON(ctx context.Context, q *queen.Queen, queue string, in io.Reader, batchSize int) (queued, dup, failed int, err error) {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)

	buf := make([]pushItem, 0, batchSize)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if pushDryRun {
			queued += len(buf)
			buf = buf[:0]
			return nil
		}
		req := map[string]any{"items": []map[string]any{}}
		items := make([]map[string]any, 0, len(buf))
		for _, it := range buf {
			m := map[string]any{
				"queue":   it.queue,
				"payload": it.payload,
			}
			if it.partition != "" {
				m["partition"] = it.partition
			}
			if it.transactionID != "" {
				m["transactionId"] = it.transactionID
			}
			if it.traceID != "" {
				m["traceId"] = it.traceID
			}
			items = append(items, m)
		}
		req["items"] = items
		resp, perr := q.GetHttpClient().Post(ctx, "/api/v1/push", req)
		if perr != nil {
			return fmt.Errorf("push to %s: %w", queue, perr)
		}
		// The push response shape is {data: [{status, transactionId, ...}]}
		// or sometimes the array is at the top level.
		entries, _ := resp["data"].([]any)
		if entries == nil {
			if arr, ok := resp["raw"].(string); ok {
				return fmt.Errorf("unexpected response: %s", arr)
			}
			// Some responses come as a bare array; not common via the SDK
			// HTTP client (it wraps under "data"), but tolerate it.
		}
		for _, e := range entries {
			row, _ := e.(map[string]any)
			status, _ := row["status"].(string)
			switch status {
			case "queued", "buffered":
				queued++
			case "duplicate":
				dup++
			default:
				failed++
			}
		}
		buf = buf[:0]
		return nil
	}

	firstTxnAssigned := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var raw any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return queued, dup, failed, clierr.Userf("invalid JSON line: %v", err)
		}

		var (
			payload   any
			partition = pushPartition
			txnID     string
			traceID   = pushTrace
		)
		if pushWrapped {
			b, _ := json.Marshal(raw)
			var w wrappedItem
			if err := json.Unmarshal(b, &w); err != nil {
				return queued, dup, failed, clierr.Userf("--wrapped: invalid item: %v", err)
			}
			payload = w.Data
			if w.Partition != "" {
				partition = w.Partition
			}
			if w.TransactionID != "" {
				txnID = w.TransactionID
			}
			if w.TraceID != "" {
				traceID = w.TraceID
			}
		} else {
			payload = raw
		}
		if pushPartitionKey != "" {
			if v := lookupPartitionKey(payload, pushPartitionKey); v != "" {
				partition = v
			}
		}
		// --transaction-id applies to the first non-wrapped item, mirroring
		// the SDK PushBuilder semantics.
		if pushTxID != "" && !pushWrapped && !firstTxnAssigned {
			txnID = pushTxID
			firstTxnAssigned = true
		}
		// Server-side push_messages_v3 generates a fresh UUID when
		// transactionId is empty. We could mint one here for parity with the
		// SDK, but letting the server own that keeps behaviour consistent
		// across clients (same as a curl-driven push).
		buf = append(buf, pushItem{
			queue:         queue,
			partition:     partition,
			payload:       payload,
			transactionID: txnID,
			traceID:       traceID,
		})
		if len(buf) >= batchSize {
			if err := flush(); err != nil {
				return queued, dup, failed, clierr.Server(err)
			}
		}
	}
	if scerr := scanner.Err(); scerr != nil {
		if errors.Is(scerr, bufio.ErrTooLong) {
			return queued, dup, failed, clierr.Userf("input line too long; pre-split with jq -c")
		}
		return queued, dup, failed, clierr.Userf("read input: %v", scerr)
	}
	if err := flush(); err != nil {
		return queued, dup, failed, clierr.Server(err)
	}
	_ = time.Time{}
	return queued, dup, failed, nil
}

func lookupPartitionKey(payload any, key string) string {
	parts := strings.Split(key, ".")
	cur := payload
	for _, p := range parts {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	switch v := cur.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%v", v)
	case bool:
		return fmt.Sprintf("%v", v)
	default:
		return ""
	}
}

func init() {
	pushCmd.Flags().StringVar(&pushPartition, "partition", "", "static partition for every message")
	pushCmd.Flags().StringVar(&pushPartitionKey, "partition-key", "", "JSON path inside each payload to use as partition (e.g. user_id, order.id)")
	pushCmd.Flags().IntVar(&pushBatch, "batch", 100, "messages per HTTP request")
	pushCmd.Flags().StringVar(&pushData, "data", "", "single inline JSON payload (skips stdin)")
	pushCmd.Flags().StringVarP(&pushFile, "file", "f", "", "read NDJSON from file instead of stdin")
	pushCmd.Flags().StringVar(&pushTrace, "trace-id", "", "attach this UUID as the trace ID")
	pushCmd.Flags().StringVar(&pushTxID, "transaction-id", "", "explicit transactionId for the first item (SDK semantics)")
	pushCmd.Flags().BoolVar(&pushWrapped, "wrapped", false, "input lines are full SDK items: {transactionId?, traceId?, partition?, data}")
	pushCmd.Flags().BoolVar(&pushDryRun, "dry-run", false, "parse + group input, do not send")
	rootCmd.AddCommand(pushCmd)
}
