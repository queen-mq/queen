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
// server/src/main.rs). It is NOT idempotent, which is why it needs --yes and
// why the SDK call underneath opts out of the client's 5xx retry loop -- the
// reasoning is on Admin.RetryMessage and summarised in the Long help below.
var dlqRetryCmd = &cobra.Command{
	Use:     "retry <partitionId> <transactionId>",
	Aliases: []string{"requeue", "replay"},
	Short:   "Replay one dead-lettered message back onto its queue",
	Long: `Asks the broker to re-push the dead-letter snapshot for this address to
its own queue and partition, then drop the DLQ row once the push has been
accepted. This is the broker's own replay route; the dashboard's dead-letter
view does not expose it (it offers Purge only), so queenctl and the admin
SDKs are the only wrappers.

NOT IDEMPOTENT. Run it once per address and read the result before
re-running:

  - The replay is a NEW message with a fresh transaction id, because the
    original id would be swallowed by the dedup window. Two runs mean two
    copies, and nothing on the broker collapses them.
  - It lands at the tail of the partition, so it is out of order with
    respect to its own key.
  - If the push is accepted but the DLQ cleanup then fails, the broker
    answers with an error saying the message was replayed AND is still
    dead-lettered. Running the command again in that state replays it a
    second time. queenctl does not resend it for you.
  - On an address dead-lettered by several consumer groups, the broker
    replays the most recent snapshot and deletes every group's snapshot
    for that address.

Use 'dlq list' to find addresses and 'dlq describe' to read one first.
Pass --dry-run to print the address without sending anything.`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if dlqDryRun {
			fmt.Fprintf(stdout(), "[dry-run] retry %s %s\n", args[0], args[1])
			return nil
		}
		if !dlqYes {
			return clierr.Userf("refusing to replay without --yes: retry is not idempotent (use --dry-run for a preview)")
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.RetryMessage(context.Background(), args[0], args[1])
		if err != nil {
			// The 500 that means "replayed, but the DLQ row is still there"
			// is the one an operator must not answer by re-running. Say so
			// on the way out; the raw body follows in the error itself.
			var he *queen.HTTPError
			if errors.As(err, &he) && strings.Contains(he.Body, `"replayed":true`) {
				fmt.Fprintln(os.Stderr, "WARNING: the message was replayed but its DLQ row was NOT removed.")
				fmt.Fprintln(os.Stderr, "Do not re-run this command for the same address: it would replay a second copy.")
				fmt.Fprintln(os.Stderr, "Delete the row with 'queenctl messages delete' once you have confirmed the replay.")
			}
			return clierr.Server(err)
		}
		// dlqRowRemoved:false on a SUCCESS means the row disappeared between
		// the broker's read and its delete -- i.e. something else replayed or
		// drained the same address concurrently, and the message may now exist
		// twice on the queue.
		if removed, ok := data["dlqRowRemoved"].(bool); ok && !removed {
			fmt.Fprintln(os.Stderr, "WARNING: replayed, but the DLQ row was already gone -- another retry or drain raced this one.")
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
