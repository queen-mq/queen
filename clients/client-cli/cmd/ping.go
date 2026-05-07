package cmd

import (
	"context"
	"fmt"
	"time"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	"github.com/smartpricing/queen/client-cli/internal/output"
	"github.com/spf13/cobra"
)

var pingTimeout time.Duration

var pingCmd = &cobra.Command{
	Use:   "ping",
	Short: "Health-check the broker and exit non-zero on failure",
	Long: `Calls GET /health on the configured server. Exits 0 when the
server reports 'healthy' (database connected), 2 otherwise. Useful for
liveness probes in shell scripts and CI pipelines.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
		defer cancel()
		start := time.Now()
		h, err := c.A.Health(ctx)
		latency := time.Since(start)
		if err != nil {
			return clierr.Server(fmt.Errorf("server unreachable: %w", err))
		}
		status, _ := h["status"].(string)
		db, _ := h["database"].(string)
		if status != "healthy" || (db != "" && db != "connected") {
			return clierr.Server(fmt.Errorf("unhealthy: status=%s database=%s", status, db))
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "STATUS"},
				{Header: "DATABASE"},
				{Header: "VERSION"},
				{Header: "LATENCY"},
			},
		}
		r, err := rendererFor(view, stdout())
		if err != nil {
			return err
		}
		row := map[string]any{
			"status":   status,
			"database": db,
			"version":  h["version"],
			"latency":  latency.Round(time.Millisecond).String(),
		}
		return r.Render(row)
	},
}

func init() {
	pingCmd.Flags().DurationVar(&pingTimeout, "timeout", 5*time.Second, "ping timeout")
	rootCmd.AddCommand(pingCmd)
}
