package cmd

import (
	"context"
	"fmt"
	"time"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/smartpricing/queen/clients/client-cli/internal/output"
	"github.com/spf13/cobra"
)

var pingTimeout time.Duration

var pingCmd = &cobra.Command{
	Use:   "ping",
	Short: "Health-check the broker and exit non-zero on failure",
	Long: `Calls GET /health on the configured server. Exits 0 when the
server reports 'healthy' (a leader is known and the node is caught up),
2 otherwise. Useful for liveness probes in shell scripts and CI pipelines.`,
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
		engine, _ := h["engine"].(string)
		if status != "healthy" {
			return clierr.Server(fmt.Errorf("unhealthy: status=%s", status))
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "STATUS"},
				{Header: "ENGINE"},
				{Header: "VERSION"},
				{Header: "LATENCY"},
			},
		}
		r, err := rendererFor(view, stdout())
		if err != nil {
			return err
		}
		row := map[string]any{
			"status":  status,
			"engine":  engine,
			"version": h["version"],
			"latency": latency.Round(time.Millisecond).String(),
		}
		return r.Render(row)
	},
}

func init() {
	pingCmd.Flags().DurationVar(&pingTimeout, "timeout", 5*time.Second, "ping timeout")
	rootCmd.AddCommand(pingCmd)
}
