package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/spf13/cobra"
)

var (
	txFile   string
	txDryRun bool
)

// txBundle is the on-disk shape consumed by 'queenctl tx -f'. It maps
// directly to POST /api/v1/transaction: a list of operations + a list of
// required leases. Same as the API but JSON/YAML accepted, validated
// before dispatch.
//
// {
//   "operations": [
//     {"type":"ack","transactionId":"...","partitionId":"...","status":"completed"},
//     {"type":"push","items":[{"queue":"orders","payload":{"id":1}}]}
//   ],
//   "requiredLeases": ["lease-uuid"]
// }
type txBundle struct {
	Operations     []queen.Operation `json:"operations"`
	RequiredLeases []string          `json:"requiredLeases"`
}

var txCmd = &cobra.Command{
	Use:   "tx -f <bundle.json>",
	Short: "Run an atomic ack+push transaction from a JSON file",
	Long: `Reads a transaction bundle from a file (or stdin with -f -) and
posts it to /api/v1/transaction. The whole bundle commits as one server-side
transaction backed by a single Postgres BEGIN/COMMIT.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if txFile == "" {
			return clierr.Userf("--file is required")
		}
		var src io.Reader
		if txFile == "-" {
			src = os.Stdin
		} else {
			f, err := os.Open(txFile)
			if err != nil {
				return clierr.Userf("open %s: %v", txFile, err)
			}
			defer f.Close()
			src = f
		}
		body, err := io.ReadAll(src)
		if err != nil {
			return clierr.Userf("read: %v", err)
		}
		var bundle txBundle
		if err := json.Unmarshal(body, &bundle); err != nil {
			return clierr.Userf("parse: %v", err)
		}
		if len(bundle.Operations) == 0 {
			return clierr.Userf("transaction has no operations")
		}
		if txDryRun {
			fmt.Fprintf(stdout(), "[dry-run] %d operations, %d required leases\n",
				len(bundle.Operations), len(bundle.RequiredLeases))
			return nil
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		tb := c.Q.Transaction()
		for _, op := range bundle.Operations {
			switch op.Type {
			case "ack":
				msg := &queen.Message{
					TransactionID: op.TransactionID,
					PartitionID:   op.PartitionID,
				}
				tb = tb.Ack(msg, op.Status, queen.AckOptions{ConsumerGroup: op.ConsumerGroup})
			case "push":
				if len(op.Items) == 0 {
					return clierr.Userf("push op has no items")
				}
				// Group push items by queue+partition since the builder is
				// scoped per queue.
				type key struct{ q, p string }
				groups := map[key][]any{}
				orders := []key{}
				for _, item := range op.Items {
					k := key{q: item.Queue, p: item.Partition}
					if _, ok := groups[k]; !ok {
						orders = append(orders, k)
					}
					groups[k] = append(groups[k], item.Payload)
				}
				for _, k := range orders {
					qb := tb.Queue(k.q)
					if k.p != "" {
						qb = qb.Partition(k.p)
					}
					tb = qb.Push(groups[k])
				}
			default:
				return clierr.Userf("unsupported op type %q", op.Type)
			}
		}

		resp, err := tb.Commit(context.Background())
		if err != nil {
			return clierr.Server(err)
		}
		if !resp.Success {
			return clierr.Server(fmt.Errorf("transaction failed: %s", resp.Error))
		}
		if !quiet() {
			fmt.Fprintln(stdout(), "transaction committed")
		}
		return nil
	},
}

func init() {
	txCmd.Flags().StringVarP(&txFile, "file", "f", "", "transaction bundle path (- for stdin)")
	txCmd.Flags().BoolVar(&txDryRun, "dry-run", false, "parse and validate, do not commit")
	rootCmd.AddCommand(txCmd)
}
