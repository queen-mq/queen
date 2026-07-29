package cmd

import (
	"context"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/smartpricing/queen/clients/client-cli/internal/output"
	"github.com/smartpricing/queen/clients/client-cli/internal/timefmt"
	"github.com/spf13/cobra"
)

var (
	anFrom string
	anTo   string
)

var analyticsCmd = &cobra.Command{
	Use:     "analytics",
	Aliases: []string{"a", "an"},
	Short:   "Time-series analytics by queue, system, worker, etc.",
}

func analyticsRange() (string, string, error) {
	var from, to string
	if anFrom != "" {
		t, err := timefmt.Parse(anFrom)
		if err != nil {
			return "", "", clierr.User(err)
		}
		from = timefmt.FormatRFC3339(t)
	}
	if anTo != "" {
		t, err := timefmt.Parse(anTo)
		if err != nil {
			return "", "", clierr.User(err)
		}
		to = timefmt.FormatRFC3339(t)
	}
	return from, to, nil
}

// systemAnalyticsAlternative is the hint for the three series the proxy keeps
// closed: they aggregate host and Postgres internals shared by every tenant
// on a cell, so there is nothing to scope. The queue-shaped series are
// tenant-scoped broker-side and stay open.
const systemAnalyticsAlternative = "host and Postgres internals are shared across tenants; " +
	"use 'queenctl analytics queue-ops' / 'queenctl analytics queue-lag' for per-queue series"

// analyticsRunE builds the RunE for one analytics sub-command. `surface` is
// the broker route it reads, so a proxy that blocks the route can be reported
// as such instead of as a bare 404.
func analyticsRunE(surface, alt string, fetch func(ctx context.Context, c clientHandle, from, to string) (map[string]any, error)) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		from, to, err := analyticsRange()
		if err != nil {
			return err
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := fetch(context.Background(), c, from, to)
		if err != nil {
			return blockedErr(err, surface, alt)
		}
		r, err := rendererFor(output.View{}, stdout())
		if err != nil {
			return err
		}
		if r.Format == output.FormatTable {
			r.Format = output.FormatYAML
		}
		return r.Render(data)
	}
}

// clientHandle is the subset of *sdk.Client used by analytics fetchers.
type clientHandle = *sdkClient

// sdkClient mirrors the alias for readability; declared in sdk_alias.go.

var analyticsQueueLagCmd = &cobra.Command{
	Use:   "queue-lag",
	Short: "Per-queue pop lag time series",
	RunE: analyticsRunE("GET /api/v1/analytics/queue-lag", "", func(ctx context.Context, c clientHandle, from, to string) (map[string]any, error) {
		return c.A.GetQueueLagAnalytics(ctx, from, to)
	}),
}

var analyticsQueueOpsCmd = &cobra.Command{
	Use:   "queue-ops",
	Short: "Per-queue push/pop/ack ops time series",
	RunE: analyticsRunE("GET /api/v1/analytics/queue-ops", "", func(ctx context.Context, c clientHandle, from, to string) (map[string]any, error) {
		return c.A.GetQueueOpsAnalytics(ctx, from, to)
	}),
}

var analyticsParkedCmd = &cobra.Command{
	Use:   "queue-parked",
	Short: "Per-queue parked-consumer counts",
	RunE: analyticsRunE("GET /api/v1/analytics/queue-parked-replicas", "", func(ctx context.Context, c clientHandle, from, to string) (map[string]any, error) {
		return c.A.GetQueueParkedReplicas(ctx, from, to)
	}),
}

var analyticsRetentionCmd = &cobra.Command{
	Use:   "retention",
	Short: "Retention/cleanup analytics",
	RunE: analyticsRunE("GET /api/v1/analytics/retention", "", func(ctx context.Context, c clientHandle, from, to string) (map[string]any, error) {
		return c.A.GetRetentionAnalytics(ctx, from, to)
	}),
}

var analyticsSystemCmd = &cobra.Command{
	Use:   "system",
	Short: "Per-replica system metrics (operator-only through a proxy)",
	RunE: analyticsRunE("GET /api/v1/analytics/system-metrics", systemAnalyticsAlternative, func(ctx context.Context, c clientHandle, from, to string) (map[string]any, error) {
		return c.A.GetSystemMetrics(ctx, from, to)
	}),
}

var analyticsWorkerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Per-worker event-loop and DB metrics (operator-only through a proxy)",
	RunE: analyticsRunE("GET /api/v1/analytics/worker-metrics", systemAnalyticsAlternative, func(ctx context.Context, c clientHandle, from, to string) (map[string]any, error) {
		return c.A.GetWorkerMetrics(ctx, from, to)
	}),
}

var analyticsPostgresCmd = &cobra.Command{
	Use:   "postgres",
	Short: "Postgres connection / activity stats (operator-only through a proxy)",
	RunE: analyticsRunE("GET /api/v1/analytics/postgres-stats", systemAnalyticsAlternative, func(ctx context.Context, c clientHandle, from, to string) (map[string]any, error) {
		return c.A.GetPostgresStats(ctx)
	}),
}

var analyticsOverviewCmd = &cobra.Command{
	Use:   "overview",
	Short: "Composite analytics dashboard data",
	RunE: analyticsRunE("GET /api/v1/status/analytics", "", func(ctx context.Context, c clientHandle, from, to string) (map[string]any, error) {
		return c.A.GetAnalytics(ctx, from, to)
	}),
}

func init() {
	for _, cmd := range []*cobra.Command{
		analyticsQueueLagCmd, analyticsQueueOpsCmd, analyticsParkedCmd,
		analyticsRetentionCmd, analyticsSystemCmd, analyticsWorkerCmd,
		analyticsPostgresCmd, analyticsOverviewCmd,
	} {
		cmd.Flags().StringVar(&anFrom, "from", "", "start time")
		cmd.Flags().StringVar(&anTo, "to", "", "end time")
	}
	analyticsCmd.AddCommand(analyticsOverviewCmd, analyticsQueueLagCmd, analyticsQueueOpsCmd,
		analyticsParkedCmd, analyticsRetentionCmd, analyticsSystemCmd,
		analyticsWorkerCmd, analyticsPostgresCmd)
	rootCmd.AddCommand(analyticsCmd)
}
