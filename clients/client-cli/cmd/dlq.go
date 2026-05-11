package cmd

import (
	"context"
	"fmt"

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

// `dlq requeue` is intentionally absent: the broker has no
// POST /api/v1/messages/:p/:tx/retry endpoint. To replay a DLQ message,
// fetch its data via `messages get`, push it back via `push`, then delete
// the DLQ row with `messages delete`.

var dlqDrainCmd = &cobra.Command{
	Use:   "drain",
	Short: "Bulk-delete DLQ messages matching filters",
	Long: `Iterates the DLQ filtered by --queue/--cg/--partition/--from/--to
and deletes each matched row. Pass --dry-run to print what would happen
without sending requests.

Note: the broker currently has no server-side requeue/retry endpoint, so
this command's only effect is deletion. To replay messages from the DLQ
into the live queue, capture their data with 'messages get' / 'dlq list'
and re-publish via 'push'.`,
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
	dlqDrainCmd.Flags().BoolVar(&dlqDryRun, "dry-run", false, "preview without sending")
	dlqDrainCmd.Flags().BoolVar(&dlqYes, "yes", false, "confirm destructive operation")

	dlqCmd.AddCommand(dlqListCmd, dlqDescribeCmd, dlqDrainCmd)
	rootCmd.AddCommand(dlqCmd)
}
