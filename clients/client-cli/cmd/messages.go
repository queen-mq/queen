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
	msgListQueue     string
	msgListPartition string
	msgListStatus    string
	msgListCG        string
	msgListLimit     int
	msgListOffset    int

	msgDeleteYes bool
)

var messagesCmd = &cobra.Command{
	Use:     "messages",
	Aliases: []string{"msg", "msgs"},
	Short:   "Inspect, delete, retry, and DLQ-move individual messages",
}

var messagesListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List messages matching filters",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.ListMessages(context.Background(), queen.ListMessagesParams{
			Queue:         msgListQueue,
			Partition:     msgListPartition,
			Status:        msgListStatus,
			ConsumerGroup: msgListCG,
			Limit:         msgListLimit,
			Offset:        msgListOffset,
		})
		if err != nil {
			return clierr.Server(err)
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "TX", Path: "transactionId"},
				{Header: "QUEUE"},
				{Header: "PARTITION"},
				{Header: "STATUS"},
				{Header: "RETRIES", Path: "retryCount"},
				{Header: "CREATED", Path: "createdAt"},
				{Header: "PARTITIONID", Path: "partitionId", Wide: true},
				{Header: "PRODUCER", Path: "producerSub", Wide: true},
			},
			RowsFrom: func(d any) []any {
				if rows := output.AsArray(d, "messages"); rows != nil {
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

var messagesGetCmd = &cobra.Command{
	Use:     "get <partitionId> <transactionId>",
	Aliases: []string{"describe", "show"},
	Short:   "Print full message detail",
	Args:    cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.GetMessage(context.Background(), args[0], args[1])
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

var messagesDeleteCmd = &cobra.Command{
	Use:     "delete <partitionId> <transactionId>",
	Aliases: []string{"rm"},
	Short:   "Delete a single message (async via stored procedure)",
	Args:    cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if !msgDeleteYes {
			return clierr.Userf("refusing to delete without --yes")
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		if _, err := c.A.DeleteMessage(context.Background(), args[0], args[1]); err != nil {
			return clierr.Server(err)
		}
		if !quiet() {
			fmt.Fprintln(stdout(), "deleted")
		}
		return nil
	},
}

var messagesTracesCmd = &cobra.Command{
	Use:   "traces <partitionId> <transactionId>",
	Short: "Print the trace timeline for a message",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		data, err := c.A.GetTracesForMessage(context.Background(), args[0], args[1])
		if err != nil {
			return clierr.Server(err)
		}
		view := output.View{
			Columns: []output.Column{
				{Header: "AT", Path: "createdAt"},
				{Header: "EVENT", Path: "eventType"},
				{Header: "TRACE", Path: "traceName"},
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

func init() {
	messagesListCmd.Flags().StringVar(&msgListQueue, "queue", "", "filter by queue")
	messagesListCmd.Flags().StringVar(&msgListPartition, "partition", "", "filter by partition")
	messagesListCmd.Flags().StringVar(&msgListStatus, "status", "", "filter by status: pending|processing|completed|failed")
	messagesListCmd.Flags().StringVar(&msgListCG, "cg", "", "filter by consumer group")
	messagesListCmd.Flags().IntVar(&msgListLimit, "limit", 100, "max messages")
	messagesListCmd.Flags().IntVar(&msgListOffset, "offset", 0, "pagination offset")

	messagesDeleteCmd.Flags().BoolVar(&msgDeleteYes, "yes", false, "confirm destructive operation")

	messagesCmd.AddCommand(messagesListCmd, messagesGetCmd, messagesDeleteCmd,
		messagesTracesCmd)
	rootCmd.AddCommand(messagesCmd)
}
