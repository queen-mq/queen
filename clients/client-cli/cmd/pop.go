package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/spf13/cobra"
)

var (
	popGroup       string
	popPartition   string
	popLimit       int
	popBatch       int
	popMaxParts    int
	popAutoAck     bool
	popWait        bool
	popTimeout     time.Duration
	popNamespace   string
	popTask        string
	popSubMode     string
	popSubFrom     string
)

var popCmd = &cobra.Command{
	Use:   "pop <queue>",
	Short: "Pop one or more messages from a queue",
	Long: `One-shot pop. By default returns up to 1 message and prints it as
NDJSON on stdout. Use --limit to retrieve N. Use --auto-ack to have the
server ack the messages atomically with the pop.

  queenctl pop orders --auto-ack
  queenctl pop orders -n 50 --cg analyzer | jq -s 'length'`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		var qb = c.Q.Queue("")
		if len(args) == 1 {
			qb = c.Q.Queue(args[0])
		}
		if popPartition != "" {
			qb = qb.Partition(popPartition)
		}
		if popGroup != "" {
			qb = qb.Group(popGroup)
		}
		if popNamespace != "" {
			qb = qb.Namespace(popNamespace)
		}
		if popTask != "" {
			qb = qb.Task(popTask)
		}
		if popBatch > 0 {
			qb = qb.Batch(popBatch)
		} else if popLimit > 0 {
			qb = qb.Batch(popLimit)
		}
		if popMaxParts > 1 {
			qb = qb.Partitions(popMaxParts)
		}
		if popSubMode != "" {
			qb = qb.SubscriptionMode(popSubMode)
		}
		if popSubFrom != "" {
			qb = qb.SubscriptionFrom(popSubFrom)
		}
		qb = qb.AutoAck(popAutoAck).Wait(popWait).TimeoutMillis(int(popTimeout.Milliseconds()))

		ctx, cancel := context.WithTimeout(cmd.Context(), popTimeout+10*time.Second)
		defer cancel()

		messages, err := qb.Pop(ctx)
		if err != nil {
			return clierr.Server(fmt.Errorf("pop: %w", err))
		}
		if len(messages) == 0 {
			return clierr.Empty("no messages")
		}

		enc := json.NewEncoder(stdout())
		emitted := 0
		for _, msg := range messages {
			if popLimit > 0 && emitted >= popLimit {
				break
			}
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
				return clierr.Userf("encode: %v", err)
			}
			emitted++
		}
		return nil
	},
}

func init() {
	popCmd.Flags().StringVar(&popGroup, "cg", "", "consumer group")
	popCmd.Flags().StringVar(&popPartition, "partition", "", "specific partition")
	popCmd.Flags().IntVarP(&popLimit, "limit", "n", 1, "maximum messages to print")
	popCmd.Flags().IntVar(&popBatch, "batch", 0, "batch size sent to server (defaults to --limit)")
	popCmd.Flags().IntVar(&popMaxParts, "max-partitions", 1, "claim up to N partitions in one call")
	popCmd.Flags().BoolVar(&popAutoAck, "auto-ack", false, "ack server-side")
	popCmd.Flags().BoolVar(&popWait, "wait", true, "long-poll until messages arrive or timeout")
	popCmd.Flags().DurationVar(&popTimeout, "timeout", 10*time.Second, "long-poll timeout")
	popCmd.Flags().StringVar(&popNamespace, "namespace", "", "filter pop by namespace")
	popCmd.Flags().StringVar(&popTask, "task", "", "filter pop by task")
	popCmd.Flags().StringVar(&popSubMode, "from-mode", "", "subscription mode: all|new|new-only")
	popCmd.Flags().StringVar(&popSubFrom, "since", "", "subscription start: 'now', RFC3339, '5m ago'")
	rootCmd.AddCommand(popCmd)
}
