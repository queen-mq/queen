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
		r, err := rendererFor(consumerGroupsView(), stdout())
		if err != nil {
			return err
		}
		return r.Render(data)
	},
}

// consumerGroupsView is the table shape shared by `queenctl lag` and
// `queenctl cg list` (which reuses this RunE).
//
// CONFLATION is not decoration: it changes how the two columns to its left are
// read. A conflating group's MESSAGES is log depth — positions still to retire —
// while the work left is one handler run per non-empty partition
// (PLAN_CONFLATION §5.3). A group sitting at four million messages is healthy
// when that column says "yes" and an incident when it says "-", and until
// §2.6 taught get_consumer_groups_v4 to join consumer_groups_metadata there was
// no way to tell the two apart from here.
func consumerGroupsView() output.View {
	return output.View{
		Columns: []output.Column{
			{Header: "CG", Path: "name"},
			{Header: "QUEUE"},
			{Header: "MESSAGES", Path: "lagMessages", Format: output.HumanInt},
			{Header: "LAG", Path: "lagSeconds", Format: output.HumanDuration},
			{Header: "CONFLATION", Path: "conflation", Format: output.Flag},
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
}

func init() {
	lagCmd.Flags().IntVar(&lagMinSeconds, "min-seconds", 0, "only show groups with lag >= N seconds")
	rootCmd.AddCommand(lagCmd)
}
