package cmd

import (
	"context"
	"fmt"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/smartpricing/queen/clients/client-cli/internal/output"
	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/spf13/cobra"
)

var (
	partSeekCG string
	partSeekTo string
)

var partitionCmd = &cobra.Command{
	Use:     "partition",
	Aliases: []string{"partitions", "p"},
	Short:   "Inspect and manipulate individual queue partitions",
}

var partitionListCmd = &cobra.Command{
	Use:     "list <queue>",
	Aliases: []string{"ls"},
	Short:   "List partitions of a queue",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.GetPartitions(context.Background(), args[0])
		if err != nil {
			return clierr.Server(err)
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "PARTITION", Path: "name"},
				{Header: "PENDING", Path: "pending", Format: output.HumanInt},
				{Header: "PROCESSING", Path: "processing", Format: output.HumanInt},
				{Header: "COMPLETED", Path: "completed", Format: output.HumanInt, Wide: true},
				{Header: "FAILED", Path: "failed", Format: output.HumanInt, Wide: true},
				{Header: "DLQ", Path: "dlq", Format: output.HumanInt, Wide: true},
				{Header: "TOTAL", Path: "total", Format: output.HumanInt, Wide: true},
				{Header: "MESSAGES", Path: "messages.total", Format: output.HumanInt, Wide: true},
			},
			RowsFrom: func(d any) []any {
				// Server returns {queue:{...}, partitions:[...], totals:{...}}.
				if rows := output.AsArray(d, "partitions"); rows != nil {
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

var partitionDescribeCmd = &cobra.Command{
	Use:     "describe <queue> <partition>",
	Aliases: []string{"get", "show"},
	Short:   "Print detail for one partition",
	Args:    cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		// No dedicated API; filter ListMessages on the partition for a useful
		// summary view.
		data, err := c.A.ListMessages(context.Background(), queen.ListMessagesParams{
			Queue:     args[0],
			Partition: args[1],
			Limit:     50,
		})
		if err != nil {
			return clierr.Server(err)
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

var partitionSeekCmd = &cobra.Command{
	Use:   "seek <queue> <partition>",
	Short: "Seek a single partition's CG cursor to its end",
	Long: `Server-side, partition-scoped seek only supports seek-to-end (the
underlying SP queen.seek_partition_v1 has no timestamp argument). The
--to flag is therefore restricted to 'end'/'now'/'latest'. To seek to a
timestamp, use 'queenctl cg seek' (queue-wide) instead.

Server route: POST /api/v1/consumer-groups/:cg/queues/:q/partitions/:p/seek.`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if partSeekCG == "" {
			return clierr.Userf("--cg is required")
		}
		switch partSeekTo {
		case "", "end", "now", "latest":
			// supported
		default:
			return clierr.Userf("partition seek only supports 'end' (got %q); use 'queenctl cg seek' for timestamp-based seeks", partSeekTo)
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		if _, err := c.A.SeekConsumerGroupPartition(context.Background(), partSeekCG, args[0], args[1], queen.SeekConsumerGroupOptions{ToEnd: true}); err != nil {
			return clierr.Server(err)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "seek cg=%s queue=%s partition=%s to=end\n",
				partSeekCG, args[0], args[1])
		}
		return nil
	},
}

func init() {
	partitionSeekCmd.Flags().StringVar(&partSeekCG, "cg", "", "consumer group")
	partitionSeekCmd.Flags().StringVar(&partSeekTo, "to", "end", "must be 'end' (only seek-to-end is supported per partition)")
	_ = partitionSeekCmd.MarkFlagRequired("cg")

	partitionCmd.AddCommand(partitionListCmd, partitionDescribeCmd, partitionSeekCmd)
	rootCmd.AddCommand(partitionCmd)
}
