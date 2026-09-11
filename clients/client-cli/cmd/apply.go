package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	queen "github.com/smartpricing/queen/clients/client-go"
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
	Namespace string       `yaml:"namespace,omitempty"`
	Task      string       `yaml:"task,omitempty"`
	Config    *configBlock `yaml:"config,omitempty"`

	// ConsumerGroup-specific
	Queue string `yaml:"queue,omitempty"`
	// SeekTo accepts the same forms as 'queenctl replay --to': RFC3339,
	// "5m ago", "now", "beginning".
	SeekTo string `yaml:"seek-to,omitempty"`
}

// configBlock is a manifest's `config:` mapping, held as the exact set of keys
// the document wrote and the exact value it wrote for each.
//
// WHY NOT queen.QueueConfig, WHICH IS WHAT THIS USED TO BE. That struct's
// fields are plain ints and bools, so a decoded `deadLetterQueue: false` is
// indistinguishable from a key the manifest never mentioned, and the SDK's
// option builder omits every zero on its way to the wire. Under `mode:
// replace` — which is what this command sends — an omitted key is not "leave
// it alone", it is "put it back to the default". So the two lines a manifest
// author most obviously means as configuration,
//
//	deadLetterQueue: false
//	leaseTime: 0
//
// landed on the broker's defaults instead: dead-lettering stayed ON, the lease
// went back to 300s, and `apply` printed `applied`. Reading the YAML mapping
// key by key is what makes a manifest mean what it says: PRESENT is decided by
// the document, not by whether the value happens to be a Go zero.
type configBlock struct {
	// opts is the wire bag: one entry per key the document carried, holding the
	// literal value it carried. nil for an explicit YAML null, which is the
	// broker's own "restore this option's default".
	opts map[string]interface{}
	// keys is the same key set in document order, so what goes on the wire (and
	// what a test reads) does not depend on Go's map iteration.
	keys []string
}

// queueConfigKeys is the option vocabulary a `config:` block may use: every
// yaml-tagged field of queen.QueueConfig, mapped to the type its value must
// decode as.
//
// Derived by reflection rather than written out here, so the CLI cannot drift
// from the SDK: a field added to QueueConfig is accepted by manifests the day
// it lands, with its declared type checking the value, and a field renamed
// there renames the manifest key with it instead of leaving this list quietly
// accepting a spelling nothing binds.
func queueConfigKeys() map[string]reflect.Type {
	t := reflect.TypeOf(queen.QueueConfig{})
	out := make(map[string]reflect.Type, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := f.Name
		if tag := f.Tag.Get("yaml"); tag != "" {
			if n, _, _ := strings.Cut(tag, ","); n != "" {
				name = n
			}
		}
		if name == "-" {
			continue
		}
		out[name] = f.Type
	}
	return out
}

// UnmarshalYAML binds the `config:` mapping one key at a time.
//
// It does by hand what yaml's own KnownFields(true) does for the rest of the
// document — an unknown key is an error, in the decoder's own wording — because
// the struct it would otherwise decode into cannot report which keys were
// there. Each value is decoded into its field's real type, so `leaseTime: "60"`
// is still a type error rather than a string on the wire.
func (c *configBlock) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: config must be a mapping of option names to values", node.Line)
	}
	fields := queueConfigKeys()
	c.opts = make(map[string]interface{}, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode, valNode := node.Content[i], node.Content[i+1]
		var key string
		if err := keyNode.Decode(&key); err != nil {
			return fmt.Errorf("line %d: config keys must be option names: %w", keyNode.Line, err)
		}
		typ, known := fields[key]
		if !known {
			// The wording yaml.v3 uses for the same mistake one level up, so a
			// misspelling inside `config:` reads like a misspelling anywhere
			// else in the document.
			return fmt.Errorf("line %d: field %s not found in type queen.QueueConfig", keyNode.Line, key)
		}
		if _, dup := c.opts[key]; dup {
			// Two spellings of one option, where the second silently wins, is
			// the same class of quiet damage as an unknown key.
			return fmt.Errorf("line %d: config key %q is set twice", keyNode.Line, key)
		}
		c.keys = append(c.keys, key)
		if valNode.Tag == "!!null" {
			// An explicit null is the broker's "restore this option's default",
			// and it is the one value a typed field cannot carry: decoded into
			// an int it would arrive as a literal 0.
			c.opts[key] = nil
			continue
		}
		v := reflect.New(typ)
		if err := valNode.Decode(v.Interface()); err != nil {
			return fmt.Errorf("line %d: config %s: %w", valNode.Line, key, err)
		}
		c.opts[key] = v.Elem().Interface()
	}
	return nil
}

var applyCmd = &cobra.Command{
	Use:   "apply -f <file>",
	Short: "Apply queue / consumer-group manifests declaratively",
	Long: `Apply YAML or JSON manifests describing queues and consumer
groups. Operations are idempotent and tolerant of pre-existing resources.

A manifest is the WHOLE configuration of the queue it describes: a queue option
the document does not mention goes back to its default, even if a previous apply
or a 'queenctl queue configure' had set it. That is the declarative contract,
and it is the difference from 'queenctl queue configure --flag', which merges
into what the queue already has and touches only the flags you typed.

A key you write is a key that is sent, with the value you wrote: 'leaseTime: 0'
sets the lease to zero and 'deadLetterQueue: false' turns dead-lettering off,
rather than reading as an unset field and landing on the default. Only the keys
the document leaves out go back to their defaults.

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
    deadLetterQueue: false
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
		// A key this CLI cannot bind is an ERROR, not a shrug. The manifest is
		// the whole configuration of the queue it names (we send mode: replace
		// below), so a misspelled or unknown option is not "ignored": it is
		// silently reset to its default, reported as `applied`, and read off the
		// queue days later. Fail on the file instead, where the typo is.
		dec.KnownFields(true)
		applied := 0
		for {
			var doc applyDoc
			if err := dec.Decode(&doc); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return clierr.Userf("parse: %v (the config keys are spelled the "+
					"way the API spells them: leaseTime, retryLimit, maxSize, …)", err)
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
				// Replace, not merge: the document in front of us is the
				// desired state of this queue in full, so an option it does not
				// carry must go back to its default rather than survive from
				// whatever the queue happened to have. Applying the same file
				// twice, or after someone nudged an option by hand, has to
				// converge on the file — that is the only property that makes a
				// manifest worth keeping in git.
				op := qb.Create().Replace(true)
				// Option() per key, never Config(): the SDK's QueueConfig bag
				// drops every zero and every false on its way to the wire, and
				// under `replace` a dropped key is an option RESET. Option
				// sends what it is given, so the manifest's `false` and its `0`
				// arrive as a literal false and a literal zero, and the keys
				// the manifest leaves out are the only ones that go back to
				// their defaults.
				if doc.Config != nil {
					for _, k := range doc.Config.keys {
						op = op.Option(k, doc.Config.opts[k])
					}
				}
				if _, err := op.Execute(ctx); err != nil {
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
