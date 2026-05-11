package cmd

import (
	"context"

	"github.com/smartpricing/queen/clients/client-cli/internal/output"
	"github.com/spf13/cobra"
)

var lagMinSeconds int

var lagCmd = &cobra.Command{
	Use:   "lag",
	Short: "List consumer groups with their lag",
	Long: `Shows every consumer group with current lag (seconds and message
count). Use --min-seconds to filter out groups with negligible lag, useful
for alerting pipelines.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx := context.Background()
		var data map[string]any
		if lagMinSeconds > 0 {
			data, err = c.A.GetLaggingConsumers(ctx, lagMinSeconds)
		} else {
			data, err = c.A.ListConsumerGroups(ctx)
		}
		if err != nil {
			return err
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "CG", Path: "name"},
				{Header: "QUEUE"},
				{Header: "MESSAGES", Path: "lagMessages", Format: output.HumanInt},
				{Header: "LAG", Path: "lagSeconds", Format: output.HumanDuration},
				{Header: "PARTITIONS", Wide: true},
				{Header: "SUBSCRIBED", Path: "subscriptionTimestamp", Wide: true},
			},
			RowsFrom: func(d any) []any {
				if rows := output.AsArray(d, "consumerGroups"); rows != nil {
					return rows
				}
				if rows := output.AsArray(d, "groups"); rows != nil {
					return rows
				}
				if rows := output.AsArray(d, "data"); rows != nil {
					return rows
				}
				if arr, ok := d.([]any); ok {
					return arr
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

func init() {
	lagCmd.Flags().IntVar(&lagMinSeconds, "min-seconds", 0, "only show groups with lag >= N seconds")
	rootCmd.AddCommand(lagCmd)
}
