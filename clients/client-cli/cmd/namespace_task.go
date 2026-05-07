package cmd

import (
	"context"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	"github.com/smartpricing/queen/client-cli/internal/output"
	"github.com/spf13/cobra"
)

var namespaceCmd = &cobra.Command{
	Use:     "namespace",
	Aliases: []string{"namespaces", "ns"},
	Short:   "Inspect queue namespaces",
}

var namespaceListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List all namespaces with their queues",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.GetNamespaces(context.Background())
		if err != nil {
			return clierr.Server(err)
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "NAMESPACE", Path: "name"},
				{Header: "QUEUES", Path: "queueCount", Format: output.HumanInt},
				{Header: "TASKS", Path: "taskCount", Wide: true},
			},
			RowsFrom: func(d any) []any {
				if rows := output.AsArray(d, "namespaces"); rows != nil {
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

var taskCmd = &cobra.Command{
	Use:     "task",
	Aliases: []string{"tasks"},
	Short:   "Inspect queue tasks",
}

var taskListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List all tasks with their queues",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.GetTasks(context.Background())
		if err != nil {
			return clierr.Server(err)
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "TASK", Path: "name"},
				{Header: "QUEUES", Path: "queueCount", Format: output.HumanInt},
				{Header: "NAMESPACES", Path: "namespaceCount", Wide: true},
			},
			RowsFrom: func(d any) []any {
				if rows := output.AsArray(d, "tasks"); rows != nil {
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

func init() {
	namespaceCmd.AddCommand(namespaceListCmd)
	taskCmd.AddCommand(taskListCmd)
	rootCmd.AddCommand(namespaceCmd, taskCmd)
}
