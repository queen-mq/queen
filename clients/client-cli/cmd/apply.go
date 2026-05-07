package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	clierr "github.com/smartpricing/queen/client-cli/internal/errors"
	queen "github.com/smartpricing/queen/client-go"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	applyFile   string
	applyDryRun bool
)

// applyDoc is the on-disk shape consumed by 'queenctl apply -f'. Multiple
// docs can live in one YAML stream separated by '---'. Inspired by kubectl
// manifests, but flat (no apiVersion / kind dance).
type applyDoc struct {
	// Kind is "Queue" (default) or "ConsumerGroup".
	Kind string `yaml:"kind"`

	// Common
	Name string `yaml:"name"`

	// Queue-specific
	Namespace string             `yaml:"namespace,omitempty"`
	Task      string             `yaml:"task,omitempty"`
	Config    *queen.QueueConfig `yaml:"config,omitempty"`

	// ConsumerGroup-specific
	Queue string `yaml:"queue,omitempty"`
	// SeekTo accepts the same forms as 'queenctl replay --to': RFC3339,
	// "5m ago", "now", "beginning".
	SeekTo string `yaml:"seek-to,omitempty"`
}

var applyCmd = &cobra.Command{
	Use:   "apply -f <file>",
	Short: "Apply queue / consumer-group manifests declaratively",
	Long: `Apply YAML or JSON manifests describing queues and consumer
groups. Operations are idempotent and tolerant of pre-existing resources.

Multiple documents may be separated by '---'. Read from stdin with -f -.

  cat <<EOF | queenctl apply -f -
  kind: Queue
  name: orders
  namespace: billing
  task: ingest
  config:
    leaseTime: 60
    retryLimit: 5
    maxSize: 100000
  ---
  kind: ConsumerGroup
  name: analyzer
  queue: orders
  seek-to: beginning
  EOF`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if applyFile == "" {
			return clierr.Userf("--file is required")
		}
		var src io.Reader
		if applyFile == "-" {
			src = os.Stdin
		} else {
			f, err := os.Open(applyFile)
			if err != nil {
				return clierr.Userf("open %s: %v", applyFile, err)
			}
			defer f.Close()
			src = f
		}
		body, err := io.ReadAll(src)
		if err != nil {
			return clierr.Userf("read: %v", err)
		}

		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		ctx := context.Background()

		dec := yaml.NewDecoder(bytes.NewReader(body))
		applied := 0
		for {
			var doc applyDoc
			if err := dec.Decode(&doc); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return clierr.Userf("parse: %v", err)
			}
			if doc.Name == "" {
				continue
			}
			if doc.Kind == "" {
				doc.Kind = "Queue"
			}
			if applyDryRun {
				if !quiet() {
					fmt.Fprintf(stdout(), "[dry-run] would apply %s %q\n", doc.Kind, doc.Name)
				}
				applied++
				continue
			}
			switch doc.Kind {
			case "Queue", "queue":
				qb := c.Q.Queue(doc.Name)
				if doc.Namespace != "" {
					qb = qb.Namespace(doc.Namespace)
				}
				if doc.Task != "" {
					qb = qb.Task(doc.Task)
				}
				if doc.Config != nil {
					qb = qb.Config(*doc.Config)
				}
				if _, err := qb.Create().Execute(ctx); err != nil {
					return clierr.Server(fmt.Errorf("queue %s: %w", doc.Name, err))
				}
			case "ConsumerGroup", "consumerGroup", "cg":
				if doc.Queue == "" {
					return clierr.Userf("ConsumerGroup %q: 'queue' field is required", doc.Name)
				}
				if doc.SeekTo == "" {
					if !quiet() {
						fmt.Fprintf(stdout(), "[skip] ConsumerGroup %q has nothing to apply (no seek-to)\n", doc.Name)
					}
					continue
				}
				opts, err := parseSeekTo(doc.SeekTo)
				if err != nil {
					return clierr.User(fmt.Errorf("ConsumerGroup %q: seek-to: %w", doc.Name, err))
				}
				if _, err := c.A.SeekConsumerGroup(ctx, doc.Name, doc.Queue, opts); err != nil {
					return clierr.Server(fmt.Errorf("seek %s/%s: %w", doc.Name, doc.Queue, err))
				}
			default:
				return clierr.Userf("unsupported kind %q", doc.Kind)
			}
			if !quiet() {
				fmt.Fprintf(stdout(), "applied %s %q\n", doc.Kind, doc.Name)
			}
			applied++
		}
		if applied == 0 {
			return clierr.Empty("no documents applied")
		}
		return nil
	},
}

func init() {
	applyCmd.Flags().StringVarP(&applyFile, "file", "f", "", "manifest path or '-' for stdin")
	applyCmd.Flags().BoolVar(&applyDryRun, "dry-run", false, "parse and validate, do not apply")
	rootCmd.AddCommand(applyCmd)
}
