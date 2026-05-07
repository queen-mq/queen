package cmd

import (
	"context"
	"fmt"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	"github.com/smartpricing/queen/client-cli/internal/output"
	"github.com/spf13/cobra"
)

var (
	cgSeekCG       string
	cgSeekQueue    string
	cgSeekTo       string
	cgSeekDryRun   bool
	cgDeleteQueue  string
	cgDeleteMeta   bool
	cgDeleteYes    bool
	cgLagMinSec    int
)

var cgCmd = &cobra.Command{
	Use:     "cg",
	Aliases: []string{"consumer-group", "consumer-groups"},
	Short:   "Inspect and manage consumer groups",
}

var cgListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List consumer groups",
	RunE:    lagCmd.RunE,
}

var cgDescribeCmd = &cobra.Command{
	Use:     "describe <name>",
	Aliases: []string{"get", "show"},
	Short:   "Print consumer group detail",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.GetConsumerGroup(context.Background(), args[0])
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

var cgLagCmd = &cobra.Command{
	Use:   "lag",
	Short: "Show lagging consumer groups (alias for top-level 'queenctl lag')",
	RunE: func(cmd *cobra.Command, args []string) error {
		lagMinSeconds = cgLagMinSec
		return lagCmd.RunE(cmd, args)
	},
}

var cgSeekCmd = &cobra.Command{
	Use:   "seek <cg> <queue>",
	Short: "Seek a consumer group on a queue (same as 'queenctl replay')",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		cgSeekCG = args[0]
		cgSeekQueue = args[1]
		opts, err := parseSeekTo(cgSeekTo)
		if err != nil {
			return err
		}
		if cgSeekDryRun {
			fmt.Fprintf(stdout(), "[dry-run] would seek cg=%s queue=%s toEnd=%v ts=%q\n",
				cgSeekCG, cgSeekQueue, opts.ToEnd, opts.Timestamp)
			return nil
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		if _, err := c.A.SeekConsumerGroup(context.Background(), cgSeekCG, cgSeekQueue, opts); err != nil {
			return clierr.Server(err)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "seek cg=%s queue=%s to=%s\n", cgSeekCG, cgSeekQueue, cgSeekTo)
		}
		return nil
	},
}

var cgDeleteCmd = &cobra.Command{
	Use:     "delete <cg>",
	Aliases: []string{"rm"},
	Short:   "Delete a consumer group (--queue required to scope per-queue removal)",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if !cgDeleteYes {
			return clierr.Userf("refusing to delete without --yes")
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx := context.Background()
		if cgDeleteQueue == "" {
			if err := c.Q.DeleteConsumerGroup(ctx, args[0], cgDeleteMeta); err != nil {
				return clierr.Server(err)
			}
		} else {
			if _, err := c.A.DeleteConsumerGroupForQueue(ctx, args[0], cgDeleteQueue, cgDeleteMeta); err != nil {
				return clierr.Server(err)
			}
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "deleted cg=%s\n", args[0])
		}
		return nil
	},
}

var cgRefreshStatsCmd = &cobra.Command{
	Use:   "refresh-stats",
	Short: "Force a refresh of the consumer-group stats tables",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		if _, err := c.A.RefreshConsumerStats(context.Background()); err != nil {
			return clierr.Server(err)
		}
		if !quiet() {
			fmt.Fprintln(stdout(), "refreshed")
		}
		return nil
	},
}

func init() {
	cgLagCmd.Flags().IntVar(&cgLagMinSec, "min-seconds", 0, "filter to lag >= N seconds")
	cgSeekCmd.Flags().StringVar(&cgSeekTo, "to", "", "target time")
	cgSeekCmd.Flags().BoolVar(&cgSeekDryRun, "dry-run", false, "print, don't send")
	cgDeleteCmd.Flags().StringVar(&cgDeleteQueue, "queue", "", "scope deletion to one queue")
	cgDeleteCmd.Flags().BoolVar(&cgDeleteMeta, "metadata", false, "also delete metadata rows")
	cgDeleteCmd.Flags().BoolVar(&cgDeleteYes, "yes", false, "confirm destructive operation")

	cgCmd.AddCommand(cgListCmd, cgDescribeCmd, cgLagCmd, cgSeekCmd, cgDeleteCmd, cgRefreshStatsCmd)
	rootCmd.AddCommand(cgCmd)
}
