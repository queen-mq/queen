package cmd

import (
	"context"
	"fmt"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	"github.com/smartpricing/queen/client-cli/internal/output"
	queen "github.com/smartpricing/queen/client-go"
	"github.com/spf13/cobra"
)

var (
	queueListNamespace string
	queueListTask      string
	queueListLimit     int
	queueListOffset    int

	queueCfgNamespace                 string
	queueCfgTask                      string
	queueCfgLeaseTime                 int
	queueCfgRetryLimit                int
	queueCfgPriority                  int
	queueCfgDelayedProcessing         int
	queueCfgWindowBuffer              int
	queueCfgMaxSize                   int
	queueCfgRetentionSeconds          int
	queueCfgCompletedRetentionSeconds int
	queueCfgEncryption                bool

	queueDeleteYes bool
)

var queueCmd = &cobra.Command{
	Use:     "queue",
	Aliases: []string{"queues", "q"},
	Short:   "Inspect, configure, and delete queues",
}

var queueListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List queues",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.ListQueues(context.Background(), queen.ListQueuesParams{
			Namespace: queueListNamespace,
			Task:      queueListTask,
			Limit:     queueListLimit,
			Offset:    queueListOffset,
		})
		if err != nil {
			return clierr.Server(err)
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "NAME", Path: "name"},
				{Header: "NAMESPACE"},
				{Header: "TASK"},
				{Header: "PARTITIONS", Path: "partitionCount", Format: output.HumanInt},
				{Header: "PRIORITY"},
				{Header: "LEASE", Path: "leaseTime"},
				{Header: "RETRY", Path: "retryLimit", Wide: true},
				{Header: "MAXSIZE", Path: "maxSize", Format: output.HumanInt, Wide: true},
				{Header: "ENCRYPTED", Path: "encryptionEnabled", Wide: true},
			},
			RowsFrom: func(d any) []any {
				if rows := output.AsArray(d, "queues"); rows != nil {
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

var queueDescribeCmd = &cobra.Command{
	Use:     "describe <queue>",
	Aliases: []string{"get", "show"},
	Short:   "Print full queue detail",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.GetQueue(context.Background(), args[0])
		if err != nil {
			return clierr.Server(err)
		}
		// /api/v1/resources/queues/:queue replies 200 + {"error":"Queue not
		// found"} for a missing queue. Map that to a real error so callers
		// (and tests) can rely on the exit code.
		if errMsg, ok := data["error"].(string); ok && errMsg != "" {
			return clierr.Server(fmt.Errorf("%s", errMsg))
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

var queueConfigureCmd = &cobra.Command{
	Use:   "configure <queue>",
	Short: "Create or reconfigure a queue (POST /api/v1/configure)",
	Long: `Idempotent. Reuses 'queenctl apply' under the hood for declarative
flow, but exposes a flag-driven shortcut for one-off scripting.

With -o {json|yaml} the full server response is rendered (including the
'options' echo block useful for asserting the broker accepted the config
in tests).`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		qb := c.Q.Queue(args[0])
		if queueCfgNamespace != "" {
			qb = qb.Namespace(queueCfgNamespace)
		}
		if queueCfgTask != "" {
			qb = qb.Task(queueCfgTask)
		}
		cfg := queen.QueueConfig{
			LeaseTime:                 queueCfgLeaseTime,
			RetryLimit:                queueCfgRetryLimit,
			Priority:                  queueCfgPriority,
			DelayedProcessing:         queueCfgDelayedProcessing,
			WindowBuffer:              queueCfgWindowBuffer,
			MaxSize:                   queueCfgMaxSize,
			RetentionSeconds:          queueCfgRetentionSeconds,
			CompletedRetentionSeconds: queueCfgCompletedRetentionSeconds,
			EncryptionEnabled:         queueCfgEncryption,
		}
		qb = qb.Config(cfg)
		resp, err := qb.Create().Execute(context.Background())
		if err != nil {
			return clierr.Server(err)
		}
		// When the user asks for structured output, render the full
		// configure response so tests / scripts can inspect the echoed
		// options block. Default behavior prints a one-liner.
		if cmd.Flags().Changed("output") || gf.output != "" {
			r, err := rendererFor(output.View{}, stdout())
			if err != nil {
				return err
			}
			if r.Format == output.FormatTable {
				r.Format = output.FormatJSON
			}
			return r.Render(resp)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "configured %q\n", args[0])
		}
		return nil
	},
}

var queueDeleteCmd = &cobra.Command{
	Use:     "delete <queue>",
	Aliases: []string{"rm"},
	Short:   "Delete a queue and all its messages",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if !queueDeleteYes {
			return clierr.Userf("refusing to delete %q without --yes", args[0])
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		if _, err := c.Q.Queue(args[0]).Delete().Execute(context.Background()); err != nil {
			return clierr.Server(err)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "deleted %q\n", args[0])
		}
		return nil
	},
}

var queueStatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Aggregate queue statistics by namespace/task",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.GetQueueStats(context.Background(), queueListNamespace, queueListTask)
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

func init() {
	queueListCmd.Flags().StringVar(&queueListNamespace, "namespace", "", "filter by namespace")
	queueListCmd.Flags().StringVar(&queueListTask, "task", "", "filter by task")
	queueListCmd.Flags().IntVar(&queueListLimit, "limit", 0, "max queues returned")
	queueListCmd.Flags().IntVar(&queueListOffset, "offset", 0, "pagination offset")

	queueConfigureCmd.Flags().StringVar(&queueCfgNamespace, "namespace", "", "queue namespace")
	queueConfigureCmd.Flags().StringVar(&queueCfgTask, "task", "", "queue task")
	queueConfigureCmd.Flags().IntVar(&queueCfgLeaseTime, "lease-time", 0, "lease seconds")
	queueConfigureCmd.Flags().IntVar(&queueCfgRetryLimit, "retry-limit", 0, "retries before DLQ")
	queueConfigureCmd.Flags().IntVar(&queueCfgPriority, "priority", 0, "queue priority")
	queueConfigureCmd.Flags().IntVar(&queueCfgDelayedProcessing, "delayed-processing", 0, "delayed-processing seconds")
	queueConfigureCmd.Flags().IntVar(&queueCfgWindowBuffer, "window-buffer", 0, "window buffer seconds")
	queueConfigureCmd.Flags().IntVar(&queueCfgMaxSize, "max-size", 0, "queue capacity (0 = unlimited)")
	queueConfigureCmd.Flags().IntVar(&queueCfgRetentionSeconds, "retention", 0, "pending retention seconds")
	queueConfigureCmd.Flags().IntVar(&queueCfgCompletedRetentionSeconds, "completed-retention", 0, "completed retention seconds")
	queueConfigureCmd.Flags().BoolVar(&queueCfgEncryption, "encrypt", false, "enable payload encryption")

	queueDeleteCmd.Flags().BoolVar(&queueDeleteYes, "yes", false, "confirm destructive operation")

	queueStatsCmd.Flags().StringVar(&queueListNamespace, "namespace", "", "filter by namespace")
	queueStatsCmd.Flags().StringVar(&queueListTask, "task", "", "filter by task")

	queueCmd.AddCommand(queueListCmd, queueDescribeCmd, queueConfigureCmd,
		queueDeleteCmd, queueStatsCmd)
	rootCmd.AddCommand(queueCmd)
}
