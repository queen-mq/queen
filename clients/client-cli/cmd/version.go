package cmd

import (
	"context"
	"fmt"
	"time"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/spf13/cobra"
)

var versionShort bool

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print queenctl + server versions",
	Long: `Print the embedded build metadata for queenctl, then probe the
configured server for its version. Pass --short to print just the CLI
version without contacting the server.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Fprintf(stdout(), "queenctl %s (commit %s, built %s)\n",
			BuildVersion, BuildCommit, BuildDate)
		if versionShort {
			return nil
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h, err := c.A.Health(ctx)
		if err != nil {
			return clierr.Server(fmt.Errorf("server: %w", err))
		}
		ver, _ := h["version"].(string)
		status, _ := h["status"].(string)
		db, _ := h["database"].(string)
		fmt.Fprintf(stdout(), "server   %s\n", strOrDash(ver))
		fmt.Fprintf(stdout(), "status   %s\n", strOrDash(status))
		fmt.Fprintf(stdout(), "database %s\n", strOrDash(db))
		fmt.Fprintf(stdout(), "context  %s (%s)\n", strOrDash(c.Context), c.Server)
		return nil
	},
}

func strOrDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func init() {
	versionCmd.Flags().BoolVar(&versionShort, "short", false, "print only queenctl version")
	rootCmd.AddCommand(versionCmd)
}
