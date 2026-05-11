package cmd

import (
	"context"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/smartpricing/queen/clients/client-cli/internal/output"
	"github.com/spf13/cobra"
)

var (
	traceLimit  int
	traceOffset int
)

var tracesCmd = &cobra.Command{
	Use:   "traces",
	Short: "Inspect message trace events",
}

var tracesNamesCmd = &cobra.Command{
	Use:     "names",
	Aliases: []string{"list", "ls"},
	Short:   "List all known trace names",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.GetTraceNames(context.Background(), traceLimit, traceOffset)
		if err != nil {
			return clierr.Server(err)
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "NAME", Path: "name"},
				{Header: "EVENTS", Path: "count", Format: output.HumanInt},
				{Header: "FIRST", Path: "firstSeen"},
				{Header: "LAST", Path: "lastSeen"},
			},
			RowsFrom: func(d any) []any {
				if rows := output.AsArray(d, "names"); rows != nil {
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

var tracesByNameCmd = &cobra.Command{
	Use:   "by-name <name>",
	Short: "List traces for a given name",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.GetTracesByName(context.Background(), args[0], traceLimit, traceOffset)
		if err != nil {
			return clierr.Server(err)
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "AT", Path: "createdAt"},
				{Header: "EVENT", Path: "eventType"},
				{Header: "TX", Path: "transactionId"},
				{Header: "PARTITION", Path: "partition"},
				{Header: "DATA"},
			},
			RowsFrom: func(d any) []any {
				if rows := output.AsArray(d, "traces"); rows != nil {
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

var tracesByMessageCmd = &cobra.Command{
	Use:   "by-message <partitionId> <transactionId>",
	Short: "List traces for a single message (alias of 'messages traces')",
	Args:  cobra.ExactArgs(2),
	RunE:  messagesTracesCmd.RunE,
}

func init() {
	for _, c := range []*cobra.Command{tracesNamesCmd, tracesByNameCmd} {
		c.Flags().IntVar(&traceLimit, "limit", 100, "max rows")
		c.Flags().IntVar(&traceOffset, "offset", 0, "page offset")
	}
	tracesCmd.AddCommand(tracesNamesCmd, tracesByNameCmd, tracesByMessageCmd)
	rootCmd.AddCommand(tracesCmd)
}
