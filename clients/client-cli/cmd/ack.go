package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/spf13/cobra"
)

var (
	ackPartitionID string
	ackLeaseID     string
	ackGroup       string
	ackError       string
	ackFailed      bool
	ackBatchFile   string
)

var ackCmd = &cobra.Command{
	Use:   "ack <transactionId>",
	Short: "Acknowledge a message (or batch of messages)",
	Long: `Acknowledge one message by transaction ID, or many at once via
--batch -f file. Each line of the file must be a JSON object with at least
{transactionId, partitionId, leaseId} fields - exactly the shape emitted by
'queenctl tail' and 'queenctl pop'.

  queenctl ack 0c4f...  --partition-id ord:42 --lease-id ...
  queenctl tail q --cg c -n 100 | queenctl ack --batch -

The --failed flag marks the ack as a failure (incrementing the retry
counter and eventually moving the message to the DLQ).`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx := context.Background()

		if ackBatchFile != "" {
			return ackBatch(ctx, c.Q)
		}
		if len(args) != 1 {
			return clierr.Userf("transactionId required (or use --batch)")
		}
		msg := &queen.Message{
			TransactionID: args[0],
			PartitionID:   ackPartitionID,
			LeaseID:       ackLeaseID,
		}
		responses, err := c.Q.Ack(ctx, msg, !ackFailed, queen.AckOptions{
			ConsumerGroup: ackGroup,
			Error:         ackError,
		})
		if err != nil {
			return clierr.Server(err)
		}
		if !quiet() {
			for _, r := range responses {
				if r.Success {
					fmt.Fprintln(stdout(), "ack ok")
				} else {
					fmt.Fprintln(stdout(), "ack failed:", r.Error)
				}
			}
		}
		return nil
	},
}

func ackBatch(ctx context.Context, q *queen.Queen) error {
	var src *bufio.Scanner
	if ackBatchFile == "-" {
		src = bufio.NewScanner(os.Stdin)
	} else {
		f, err := os.Open(ackBatchFile)
		if err != nil {
			return clierr.Userf("open %s: %v", ackBatchFile, err)
		}
		defer f.Close()
		src = bufio.NewScanner(f)
	}
	src.Buffer(make([]byte, 64*1024), 16*1024*1024)
	var msgs []queen.Message
	for src.Scan() {
		line := strings.TrimSpace(src.Text())
		if line == "" {
			continue
		}
		var m queen.Message
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			return clierr.Userf("invalid JSON line: %v", err)
		}
		if m.TransactionID == "" || m.PartitionID == "" {
			return clierr.Userf("each line must include transactionId and partitionId")
		}
		msgs = append(msgs, m)
	}
	if err := src.Err(); err != nil {
		return clierr.Userf("read: %v", err)
	}
	if len(msgs) == 0 {
		return clierr.Empty("no messages to ack")
	}
	responses, err := q.Ack(ctx, msgs, !ackFailed, queen.AckOptions{
		ConsumerGroup: ackGroup,
		Error:         ackError,
	})
	if err != nil {
		return clierr.Server(err)
	}
	ok, fail := 0, 0
	for _, r := range responses {
		if r.Success {
			ok++
		} else {
			fail++
		}
	}
	if !quiet() {
		fmt.Fprintf(stdout(), "ok=%d failed=%d\n", ok, fail)
	}
	return nil
}

func init() {
	ackCmd.Flags().StringVar(&ackPartitionID, "partition-id", "", "internal partition UUID (from pop output)")
	ackCmd.Flags().StringVar(&ackLeaseID, "lease-id", "", "lease UUID returned at pop time")
	ackCmd.Flags().StringVar(&ackGroup, "cg", "", "consumer group")
	ackCmd.Flags().StringVar(&ackError, "error", "", "attach error message (use with --failed)")
	ackCmd.Flags().BoolVar(&ackFailed, "failed", false, "mark as failed instead of completed")
	ackCmd.Flags().StringVar(&ackBatchFile, "batch", "", "ack many messages from NDJSON file (- for stdin)")
	rootCmd.AddCommand(ackCmd)
}
