package cmd

import (
	"context"

	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/smartpricing/queen/client-cli/internal/output"
	"github.com/spf13/cobra"
)

var (
	statusNamespace string
	statusTask      string
	statusPartitions bool
)

var statusCmd = &cobra.Command{
	Use:   "status [queue]",
	Short: "Show cluster overview or per-queue depth and lag",
	Long: `With no argument, prints the cluster-wide status: queue list with
depth, completed, failed counts, and lag. With a queue name, prints the
detailed view including per-partition counts (--partitions).`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx := context.Background()
		if len(args) == 1 {
			data, err := c.A.GetQueueDetail(ctx, args[0], statusPartitions)
			if err != nil {
				return err
			}
			view := output.View{
				Columns: []output.Column{
					{Header: "QUEUE"},
					{Header: "PENDING"},
					{Header: "PROCESSING"},
					{Header: "COMPLETED"},
					{Header: "FAILED"},
					{Header: "DLQ"},
					{Header: "PARTITIONS"},
					{Header: "LAG", Path: "lagSeconds", Format: output.HumanDuration},
				},
				RowsFrom: func(d any) []any {
					m := output.AsMap(d)
					if m == nil {
						return nil
					}
					return []any{flattenQueueRow(m)}
				},
			}
			r, err := rendererFor(view, stdout())
			if err != nil {
				return err
			}
			return r.Render(data)
		}
		params := queen.GetStatusParams{
			Queue:     "",
			Namespace: statusNamespace,
			Task:      statusTask,
		}
		data, err := c.A.GetStatus(ctx, params)
		if err != nil {
			return err
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "QUEUE"},
				{Header: "NAMESPACE"},
				{Header: "TASK", Wide: true},
				{Header: "PENDING"},
				{Header: "PROCESSING"},
				{Header: "COMPLETED"},
				{Header: "FAILED"},
				{Header: "DLQ"},
				{Header: "LAG", Path: "lagSeconds", Format: output.HumanDuration},
			},
			RowsFrom: func(d any) []any {
				rows := output.AsArray(d, "queues")
				out := make([]any, 0, len(rows))
				for _, r := range rows {
					if m := output.AsMap(r); m != nil {
						out = append(out, flattenQueueRow(m))
					}
				}
				return out
			},
		}
		r, err := rendererFor(view, stdout())
		if err != nil {
			return err
		}
		return r.Render(data)
	},
}

// flattenQueueRow normalises the various server response shapes into a flat
// map keyed by lower-case header names.
func flattenQueueRow(m map[string]any) map[string]any {
	row := map[string]any{
		"queue":      output.FirstNonNil(m, "name", "queue"),
		"namespace":  m["namespace"],
		"task":       m["task"],
		"pending":    output.FirstNonNil(m, "pending", "pendingCount"),
		"processing": output.FirstNonNil(m, "processing", "processingCount"),
		"completed":  output.FirstNonNil(m, "completed", "completedCount"),
		"failed":     output.FirstNonNil(m, "failed", "failedCount"),
		"dlq":        output.FirstNonNil(m, "dlq", "dlqCount", "deadLetterCount"),
		"partitions": output.FirstNonNil(m, "partitions", "partitionCount"),
		"lagSeconds": output.FirstNonNil(m, "lagSeconds", "lag"),
	}
	return row
}

func init() {
	statusCmd.Flags().StringVar(&statusNamespace, "namespace", "", "filter by namespace")
	statusCmd.Flags().StringVar(&statusTask, "task", "", "filter by task")
	statusCmd.Flags().BoolVar(&statusPartitions, "partitions", false, "include per-partition detail")
	rootCmd.AddCommand(statusCmd)
}
