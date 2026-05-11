package cmd

import (
	"context"
	"fmt"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/spf13/cobra"
)

var leaseCmd = &cobra.Command{
	Use:   "lease",
	Short: "Manipulate message leases",
}

var leaseExtendCmd = &cobra.Command{
	Use:   "extend <leaseId>",
	Short: "Extend a message lease (renew)",
	Long: `Hits POST /api/v1/lease/<id>/extend, the same path the SDK uses for
automatic lease renewal. Useful when a long-running consumer needs more
time before its lease expires.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		responses, err := c.Q.Renew(context.Background(), args[0])
		if err != nil {
			return clierr.Server(err)
		}
		for _, r := range responses {
			if !r.Success {
				return clierr.Server(fmt.Errorf("renew failed: %s", r.Error))
			}
			if !quiet() {
				fmt.Fprintf(stdout(), "renewed %s -> %s\n", r.LeaseID, r.NewExpiresAt.Format("2006-01-02T15:04:05Z07:00"))
			}
		}
		return nil
	},
}

func init() {
	leaseCmd.AddCommand(leaseExtendCmd)
	rootCmd.AddCommand(leaseCmd)
}
