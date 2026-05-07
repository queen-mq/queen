package cmd

import (
	"context"
	"fmt"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	"github.com/smartpricing/queen/client-cli/internal/output"
	"github.com/spf13/cobra"
)

var (
	mainPopOnly bool
	mainYes     bool
)

var maintenanceCmd = &cobra.Command{
	Use:     "maintenance",
	Aliases: []string{"maint"},
	Short:   "Inspect and toggle maintenance mode",
	Long: `Maintenance mode quiesces the broker by rejecting writes (and pops,
for full maintenance). Use --pop to scope the operation to pop-only
maintenance, leaving pushes flowing.`,
}

var maintenanceGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Print the current maintenance state",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx := context.Background()
		var data map[string]any
		if mainPopOnly {
			data, err = c.A.GetPopMaintenanceMode(ctx)
		} else {
			data, err = c.A.GetMaintenanceMode(ctx)
		}
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

var maintenanceOnCmd = &cobra.Command{
	Use:   "on",
	Short: "Enable maintenance mode",
	RunE:  toggleMaintenance(true),
}

var maintenanceOffCmd = &cobra.Command{
	Use:   "off",
	Short: "Disable maintenance mode",
	RunE:  toggleMaintenance(false),
}

func toggleMaintenance(enabled bool) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if enabled && !mainYes {
			return clierr.Userf("refusing to enable maintenance without --yes")
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx := context.Background()
		if mainPopOnly {
			_, err = c.A.SetPopMaintenanceMode(ctx, enabled)
		} else {
			_, err = c.A.SetMaintenanceMode(ctx, enabled)
		}
		if err != nil {
			return clierr.Server(err)
		}
		if !quiet() {
			scope := "maintenance"
			if mainPopOnly {
				scope = "pop-maintenance"
			}
			state := "off"
			if enabled {
				state = "on"
			}
			fmt.Fprintf(stdout(), "%s %s\n", scope, state)
		}
		return nil
	}
}

func init() {
	maintenanceCmd.PersistentFlags().BoolVar(&mainPopOnly, "pop", false, "scope to pop-only maintenance")
	maintenanceOnCmd.Flags().BoolVar(&mainYes, "yes", false, "confirm enabling maintenance")
	maintenanceCmd.AddCommand(maintenanceGetCmd, maintenanceOnCmd, maintenanceOffCmd)
	rootCmd.AddCommand(maintenanceCmd)
}
