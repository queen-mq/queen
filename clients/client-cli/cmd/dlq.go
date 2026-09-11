package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/smartpricing/queen/clients/client-cli/internal/output"
	"github.com/smartpricing/queen/clients/client-cli/internal/timefmt"
	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/spf13/cobra"
)

var (
	dlqQueue     string
	dlqCG        string
	dlqPartition string
	dlqFrom      string
	dlqTo        string
	dlqLimit     int
	dlqOffset    int
	dlqDryRun    bool
	dlqYes       bool
)

var dlqCmd = &cobra.Command{
	Use:   "dlq",
	Short: "Inspect, requeue, and drain dead-lettered messages",
}

var dlqListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List DLQ messages with filters",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		params, err := dlqParams()
		if err != nil {
			return err
		}
		data, err := c.A.ListDLQ(context.Background(), params)
		if err != nil {
			return clierr.Server(err)
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "TX", Path: "transactionId"},
				{Header: "QUEUE"},
				{Header: "PARTITION"},
				{Header: "CG", Path: "consumerGroup"},
				{Header: "RETRIES", Path: "retryCount"},
				{Header: "ERROR", Path: "errorMessage"},
				{Header: "FAILED-AT", Path: "failedAt", Wide: true},
				{Header: "PARTITIONID", Path: "partitionId", Wide: true},
			},
			RowsFrom: func(d any) []any {
				if rows := output.AsArray(d, "messages"); rows != nil {
					return rows
				}
				if rows := output.AsArray(d, "data"); rows != nil {
					return rows
				}
				return nil
			},
		}
		r, err := rendererFor(view, stdout())
		if err != nil {
			return err
		}
		return r.Render(data)
	},
}

var dlqDescribeCmd = &cobra.Command{
	Use:     "describe <partitionId> <transactionId>",
	Aliases: []string{"get"},
	Short:   "Print full detail for one DLQ message",
	Args:    cobra.ExactArgs(2),
	RunE:    messagesGetCmd.RunE,
}

// `dlq retry` wraps POST /api/v1/messages/:p/:tx/retry (registered in
// server/src/main.rs). Since the move primitive it is idempotent BY ROW -- a
// second run answers 404 rather than pushing a second copy -- but it is still a
// write that removes a record, which is why it keeps --yes, and the SDK call
// underneath still opts out of the client's 5xx retry loop so the caller reads
// the verdict itself. The reasoning is on Admin.RetryMessage.
var dlqRetryCmd = &cobra.Command{
	Use:     "retry <partitionId> <transactionId>",
	Aliases: []string{"requeue", "replay"},
	Short:   "Replay one dead-lettered message back onto its queue",
	Long: `Asks the broker to MOVE the newest dead-letter snapshot at this address
back into the log: claim the row under a lock, push the frame to its own
queue and partition, delete the row -- one transaction. The dashboard's
dead-letter view offers the same action per row; queenctl and the admin SDKs
address it by (partitionId, transactionId) instead of by row id.

It writes, and what it removes it removes for good, so read the result:

  - The replayed frame carries the transaction id 'dlq:<row id>', which is
    derived from the row and not minted per attempt: a second run of this
    command answers 404 (the row is gone with the first move) instead of
    appending a second copy.
  - It lands at the TAIL of the partition, so it is out of order with
    respect to its own key, and its age clock restarts at the destination.
  - There is no "replayed but still dead-lettered" state: the push and the
    delete commit together or not at all. A 500 that says
    "dlqRowRemoved": false means nothing happened; one whose dlqRowRemoved
    is null means the broker never learned the outcome -- re-read the DLQ
    with 'dlq list' before doing anything else.
  - On an address dead-lettered by several consumer groups, the broker
    replays the most recent snapshot and removes ONLY that group's record.
  - result "duplicate" means nothing was written and nothing was removed:
    the destination already carries that transaction id, and the broker will
    not destroy a record it did not replay.

Use 'dlq list' to find addresses and 'dlq describe' to read one first.
Pass --dry-run to print the address without sending anything.`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if dlqDryRun {
			fmt.Fprintf(stdout(), "[dry-run] retry %s %s\n", args[0], args[1])
			return nil
		}
		if !dlqYes {
			return clierr.Userf("refusing to replay without --yes: it appends a message and removes the dead-letter record (use --dry-run for a preview)")
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.RetryMessage(context.Background(), args[0], args[1])
		if err != nil {
			// The one 500 an operator must not answer by re-running blind: the
			// broker did not learn whether the move committed, so the row may
			// or may not be there. `dlqRowRemoved` is null in that body and
			// false in the one that means "the database refused, nothing
			// happened"; the raw body follows in the error itself.
			var he *queen.HTTPError
			if errors.As(err, &he) && strings.Contains(he.Body, `"dlqRowRemoved":null`) {
				fmt.Fprintln(os.Stderr, "WARNING: the broker could not say whether this move was applied.")
				fmt.Fprintln(os.Stderr, "Re-read the address with 'queenctl dlq list' before re-running: if the row is gone, the move happened.")
			}
			return clierr.Server(err)
		}
		// dlqRowRemoved:false on a SUCCESS is the `duplicate` verdict: the
		// destination's dedup window already carries this replay's transaction
		// id, so nothing was written -- and the broker will not delete a record
		// it did not replay.
		if removed, ok := data["dlqRowRemoved"].(bool); ok && !removed {
			fmt.Fprintln(os.Stderr, "WARNING: nothing was written and the dead-letter record was kept: the destination already carries this transaction id.")
			fmt.Fprintln(os.Stderr, "Read what is at the reported offset, then replay elsewhere or purge the row.")
		}
		r, err := rendererFor(output.View{}, stdout())
		if err != nil {
			return err
		}
		if r.Format == output.FormatTable {
			r.Format = output.FormatYAML
		}
		return r.Render(data)
	},
}

var dlqDrainCmd = &cobra.Command{
	Use:   "drain",
	Short: "Bulk-delete DLQ messages matching filters",
	Long: `Iterates the DLQ filtered by --queue/--cg/--partition/--from/--to
and deletes each matched row. Pass --dry-run to print what would happen
without sending requests.

Note: this command only deletes, and a drained row is gone. To replay a
message instead, use 'dlq retry <partitionId> <transactionId>' before
draining -- there is no bulk replay, deliberately, because the underlying
route is not idempotent (see 'dlq retry --help').`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if !dlqYes && !dlqDryRun {
			return clierr.Userf("refusing to drain without --yes (use --dry-run for a preview)")
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		params, err := dlqParams()
		if err != nil {
			return err
		}
		ctx := context.Background()
		processed, errs := 0, 0
		// Pull pages of `Limit` until exhausted.
		offset := 0
		page := params.Limit
		if page <= 0 {
			page = 200
			params.Limit = page
		}
		params.Offset = offset
		for {
			data, err := c.A.ListDLQ(ctx, params)
			if err != nil {
				return clierr.Server(err)
			}
			rows := output.AsArray(data, "messages")
			if rows == nil {
				rows = output.AsArray(data, "data")
			}
			if len(rows) == 0 {
				break
			}
			for _, raw := range rows {
				m := output.AsMap(raw)
				if m == nil {
					continue
				}
				partID, _ := m["partitionId"].(string)
				txID, _ := m["transactionId"].(string)
				if partID == "" || txID == "" {
					continue
				}
				if dlqDryRun {
					fmt.Fprintf(stdout(), "[dry-run] delete %s %s\n", partID, txID)
					processed++
					continue
				}
				if _, err = c.A.DeleteMessage(ctx, partID, txID); err != nil {
					errs++
					fmt.Fprintf(stdout(), "error: %s/%s: %v\n", partID, txID, err)
					continue
				}
				processed++
			}
			if len(rows) < page {
				break
			}
			offset += page
			params.Offset = offset
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "processed=%d errors=%d\n", processed, errs)
		}
		if processed == 0 {
			return clierr.Empty("no DLQ messages matched")
		}
		return nil
	},
}

func dlqParams() (queen.ListDLQParams, error) {
	p := queen.ListDLQParams{
		Queue:         dlqQueue,
		ConsumerGroup: dlqCG,
		Partition:     dlqPartition,
		Limit:         dlqLimit,
		Offset:        dlqOffset,
	}
	if dlqFrom != "" {
		t, err := timefmt.Parse(dlqFrom)
		if err != nil {
			return p, clierr.User(err)
		}
		p.From = timefmt.FormatRFC3339(t)
	}
	if dlqTo != "" {
		t, err := timefmt.Parse(dlqTo)
		if err != nil {
			return p, clierr.User(err)
		}
		p.To = timefmt.FormatRFC3339(t)
	}
	return p, nil
}

func init() {
	for _, c := range []*cobra.Command{dlqListCmd, dlqDrainCmd} {
		c.Flags().StringVar(&dlqQueue, "queue", "", "filter by queue")
		c.Flags().StringVar(&dlqCG, "cg", "", "filter by consumer group")
		c.Flags().StringVar(&dlqPartition, "partition", "", "filter by partition")
		c.Flags().StringVar(&dlqFrom, "from", "", "filter from time")
		c.Flags().StringVar(&dlqTo, "to", "", "filter to time")
		c.Flags().IntVar(&dlqLimit, "limit", 0, "page size")
		c.Flags().IntVar(&dlqOffset, "offset", 0, "page offset")
	}
	for _, c := range []*cobra.Command{dlqDrainCmd, dlqRetryCmd} {
		c.Flags().BoolVar(&dlqDryRun, "dry-run", false, "preview without sending")
		c.Flags().BoolVar(&dlqYes, "yes", false, "confirm destructive operation")
	}

	dlqCmd.AddCommand(dlqListCmd, dlqDescribeCmd, dlqRetryCmd, dlqDrainCmd)
	rootCmd.AddCommand(dlqCmd)
}
