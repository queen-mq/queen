package cmd

import (
	"context"
	"fmt"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/spf13/cobra"
)

var (
	replayQueue   string
	replayCG      string
	replayTo      string
	replayDryRun  bool
)

var replayCmd = &cobra.Command{
	Use:   "replay <queue> --cg <cg> --to '<time>'",
	Short: "Rewind a consumer group to replay messages from a point in time",
	Long: `Move a consumer group's read offset on a queue. The next pop will
return messages from --to onward. This is the cleanest way to replay a
window of events into the same downstream pipeline.

Accepts:
  --to '15m ago'
  --to 'beginning' / 'earliest'
  --to 'now' / 'end' / 'latest'
  --to '2026-05-06T12:00:00Z'

Use --dry-run to preview the change without sending it.`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 1 {
			replayQueue = args[0]
		}
		if replayQueue == "" {
			return clierr.Userf("queue is required")
		}
		if replayCG == "" {
			return clierr.Userf("--cg is required")
		}
		opts, err := parseSeekTo(replayTo)
		if err != nil {
			return err
		}
		if replayDryRun {
			fmt.Fprintf(stdout(), "[dry-run] would seek cg=%s queue=%s toEnd=%v ts=%q\n",
				replayCG, replayQueue, opts.ToEnd, opts.Timestamp)
			return nil
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		if _, err := c.A.SeekConsumerGroup(context.Background(), replayCG, replayQueue, opts); err != nil {
			return clierr.Server(err)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "replayed cg=%s queue=%s to=%s\n", replayCG, replayQueue, replayTo)
		}
		return nil
	},
}

func init() {
	replayCmd.Flags().StringVar(&replayCG, "cg", "", "consumer group to seek")
	replayCmd.Flags().StringVar(&replayTo, "to", "", "target time: '5m ago' | 'beginning' | 'now' | RFC3339")
	replayCmd.Flags().BoolVar(&replayDryRun, "dry-run", false, "print the action without sending")
	_ = replayCmd.MarkFlagRequired("cg")
	_ = replayCmd.MarkFlagRequired("to")
	rootCmd.AddCommand(replayCmd)
}
