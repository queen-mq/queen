package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/spf13/cobra"
)

var (
	tailGroup       string
	tailPartition   string
	tailFollow      bool
	tailLimit       int
	tailBatch       int
	tailMaxParts    int
	tailAutoAck     bool
	tailFromMode    string
	tailFromAt      string
	tailIdleMillis  int
	tailConcurrency int
	tailTimeout     time.Duration
)

var tailCmd = &cobra.Command{
	Use:   "tail <queue>",
	Short: "Stream messages from a queue as NDJSON",
	Long: `Live-tail a queue, printing each message as a single JSON line on
stdout. Output is always NDJSON regardless of -o, so it composes with jq /
queenctl push:

  queenctl tail orders --cg debug --follow | jq '.data'
  queenctl tail src    --auto-ack | queenctl push dst --batch 100

Use --cg to bind to a consumer group (otherwise an ephemeral CG named
"queenctl-tail" is used). With --follow the command keeps long-polling
until interrupted; without it, returns once one batch is fetched.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		queueName := args[0]
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		group := tailGroup
		if group == "" {
			group = "queenctl-tail"
		}

		qb := c.Q.Queue(queueName).Group(group)
		if tailPartition != "" {
			qb = qb.Partition(tailPartition)
		}
		if tailBatch > 0 {
			qb = qb.Batch(tailBatch)
		}
		if tailMaxParts > 1 {
			qb = qb.Partitions(tailMaxParts)
		}
		qb = qb.AutoAck(tailAutoAck)
		if tailFromMode != "" {
			qb = qb.SubscriptionMode(tailFromMode)
		}
		if tailFromAt != "" {
			qb = qb.SubscriptionFrom(tailFromAt)
		}
		if tailLimit > 0 {
			qb = qb.Limit(tailLimit)
		}
		if tailIdleMillis > 0 {
			qb = qb.IdleMillis(tailIdleMillis)
		}
		if tailConcurrency > 0 {
			qb = qb.Concurrency(tailConcurrency)
		}
		// Per-pop long-poll timeout. The SDK default is 30s, which means
		// --idle-millis takes effect only after a 30s wait; that's a
		// surprising UX. Cap the per-pop wait so idle detection is roughly
		// linear in --idle-millis.
		popWaitMillis := int(tailTimeout / time.Millisecond)
		if popWaitMillis <= 0 {
			if tailIdleMillis > 0 && tailIdleMillis < 30_000 {
				popWaitMillis = tailIdleMillis
			} else {
				popWaitMillis = 30_000
			}
		}
		qb = qb.TimeoutMillis(popWaitMillis)
		if !tailFollow && tailLimit == 0 {
			// One-shot: cap at the batch size so we don't long-poll forever.
			batch := tailBatch
			if batch == 0 {
				batch = 100
			}
			qb = qb.Limit(batch)
		}

		enc := json.NewEncoder(stdout())
		ctx := cmd.Context()
		err = qb.Consume(ctx, func(_ context.Context, msg *queen.Message) error {
			out := map[string]any{
				"queue":         msg.Queue,
				"partition":     msg.Partition,
				"transactionId": msg.TransactionID,
				"partitionId":   msg.PartitionID,
				"createdAt":     msg.CreatedAt,
				"retryCount":    msg.RetryCount,
				"data":          msg.Data,
			}
			if msg.LeaseID != "" {
				out["leaseId"] = msg.LeaseID
			}
			if msg.ProducerSub != "" {
				out["producerSub"] = msg.ProducerSub
			}
			if err := enc.Encode(out); err != nil {
				return err
			}
			return nil
		}).Execute(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			return clierr.Server(fmt.Errorf("tail: %w", err))
		}
		return nil
	},
}

func init() {
	tailCmd.Flags().StringVar(&tailGroup, "cg", "", "consumer group (default: 'queenctl-tail')")
	tailCmd.Flags().StringVar(&tailPartition, "partition", "", "single partition to tail (default: any)")
	tailCmd.Flags().BoolVarP(&tailFollow, "follow", "f", false, "keep streaming after the queue drains")
	tailCmd.Flags().IntVarP(&tailLimit, "limit", "n", 0, "stop after N messages")
	tailCmd.Flags().IntVar(&tailBatch, "batch", 0, "messages per long-poll round-trip")
	tailCmd.Flags().IntVar(&tailMaxParts, "max-partitions", 1, "claim up to N partitions per pop (v4 multi-partition)")
	tailCmd.Flags().BoolVar(&tailAutoAck, "auto-ack", false, "ack server-side as messages are emitted")
	tailCmd.Flags().StringVar(&tailFromMode, "from", "", "subscription mode: all|new|new-only")
	tailCmd.Flags().StringVar(&tailFromAt, "since", "", "subscription start: 'now', RFC3339, '5m ago'")
	tailCmd.Flags().IntVar(&tailIdleMillis, "idle-millis", 0, "stop after N ms of no messages")
	tailCmd.Flags().IntVar(&tailConcurrency, "concurrency", 0, "parallel handlers")
	tailCmd.Flags().DurationVar(&tailTimeout, "timeout", 0, "per-pop long-poll timeout (default: 30s, or --idle-millis when smaller)")
	rootCmd.AddCommand(tailCmd)
	_ = os.Stdout
}
