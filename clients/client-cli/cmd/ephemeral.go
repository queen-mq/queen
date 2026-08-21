package cmd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	"github.com/smartpricing/queen/clients/client-cli/internal/output"
	queen "github.com/smartpricing/queen/clients/client-go"
	"github.com/spf13/cobra"
)

// The ephemeral queue family (EPHEMERAL_QUEUES.md §3.1, §4), through the Go SDK.
//
// queenctl re-implements none of the semantics: the eight verbs map one to one
// onto queen.Ephemeral, the wire shapes live there, and what this file owns is
// making the two things an operator at a terminal needs — the loss contract and
// the version requirement — impossible to miss.
//
// THE LOSS CONTRACT IS WHY reset AND delete NEED --yes. On a durable queue those
// verbs would be indefensible; here they destroy nothing the class ever promised
// to keep. But an operator who typed `reset` against the wrong cell still wants
// the pause, and the confirmation text is where the contract gets restated.

var (
	ephPushPartition string
	ephPushData      string
	ephPushFile      string
	ephPushBatch     int

	ephPopGroup     string
	ephPopPartition string
	ephPopBatch     int
	ephPopWait      bool
	ephPopTimeout   time.Duration
	ephPopAutoAck   bool

	ephAckGroup  string
	ephAckStatus string
	ephAckError  string

	ephCfgMaxBytes     int64
	ephCfgMaxLength    int64
	ephCfgPolicy       string
	ephCfgTTLSeconds   int64
	ephCfgLeaseSeconds int64
	ephCfgRetryLimit   int64
	ephCfgWindowMs     int
	ephCfgWindowCount  int

	ephResetYes  bool
	ephDeleteYes bool
)

var ephemeralCmd = &cobra.Command{
	Use:     "ephemeral",
	Aliases: []string{"eph"},
	Short:   "RAM-class queues: push, pop, ack and manage ephemeral queues",
	Long: `Ephemeral queues live in broker RAM and survive nothing — not a
restart, not a crash, not a deploy, not the ownership move a membership change
causes. Treat a failover like a Redis restart.

Declared CONFIGURATION is durable and comes back after a restart, as configured
and EMPTY. There is no replay, no history, no subscription mode and no DLQ,
because none of those concepts has a referent when there is no history to have.

Delivery is not "at most once": the class picks what can be LOST, the ack mode
picks the guarantee. --auto-ack commits at delivery (at-most-once); the default
is at-least-once for as long as the owning broker incarnation lives.

Consumption semantics come from the consumer group, exactly as on a durable
queue: same --cg competes, its own --cg fans out, no --cg is queue mode.

  queenctl ephemeral push presence --data '{"user":"a","typing":true}'
  queenctl ephemeral pop presence --cg workers --wait
  queenctl ephemeral queues

Requires broker (and proxy) >= 1.1: an older one answers 404 on the whole
family, which every verb below reports as one clear upgrade error.`,
}

// ephemeralErr classifies the SDK's one family-wide failure, or returns nil when
// err is something else.
//
// Exit 1 rather than exit 2, for the same reason a proxy-blocked route is (see
// blockedErr): retrying cannot help, and the fix is to upgrade the broker or the
// proxy in front of it. Exit 2 would tell a wrapping script to back off and try
// again forever.
func ephemeralErr(err error, verb string) error {
	if err == nil || !errors.Is(err, queen.ErrEphemeralUnsupported) {
		return nil
	}
	return clierr.Userf("ephemeral %s: %v", verb, err)
}

// ephemeralFail is the one error path of this file: the upgrade case first, then
// the missing queue, then the ordinary server error.
//
// The order matters and so does the split. Both arrive as HTTP 404 and they mean
// opposite things — "this broker has no such feature" against "this broker has
// no such queue" — so a single `not found` line would send an operator reading
// version numbers over a queue name, or the reverse.
func ephemeralFail(err error, verb string) error {
	if e := ephemeralErr(err, verb); e != nil {
		return e
	}
	if errors.Is(err, queen.ErrEphemeralQueueNotFound) {
		// Exit 4, the "nothing there" code: an implicit ephemeral queue IS its
		// ring, so a name with no ring behind it has either never been used or
		// has been idle-collected, and neither is a failure.
		return clierr.Empty(fmt.Sprintf("ephemeral %s: no such queue", verb))
	}
	return clierr.Server(fmt.Errorf("ephemeral %s: %w", verb, err))
}

// ---------------------------------------------------------------------------
// push
// ---------------------------------------------------------------------------

var ephemeralPushCmd = &cobra.Command{
	Use:   "push <queue>",
	Short: "Push messages to an ephemeral queue (NDJSON on stdin or --data)",
	Long: `Push one or many messages. All-or-nothing per request.

Each input line is the message PAYLOAD, which is arbitrary JSON: the ephemeral
wire carries {payload} and nothing else — no transactionId, because there is no
dedup index to hold one.

  echo '{"user":"a"}' | queenctl ephemeral push presence
  queenctl ephemeral push presence --data '{"user":"a"}' --partition room-7

The queue does not need to exist: naming it in a push creates it implicitly
with the tenant defaults, and it is collected again when it goes empty and
quiet. --partition is omitted when not given, so the broker picks the ring.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		queue := args[0]
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		var src io.Reader
		switch {
		case ephPushData != "":
			src = strings.NewReader(ephPushData + "\n")
		case ephPushFile != "":
			f, err := os.Open(ephPushFile)
			if err != nil {
				return clierr.Userf("open %s: %v", ephPushFile, err)
			}
			defer f.Close()
			src = f
		default:
			src = os.Stdin
		}

		batchSize := ephPushBatch
		if batchSize <= 0 {
			batchSize = 100
		}

		eph := c.Q.Ephemeral()
		opts := queen.EphemeralPushOptions{Partition: ephPushPartition}
		var pushed int64
		buf := make([]queen.EphemeralMessage, 0, batchSize)

		flush := func() error {
			if len(buf) == 0 {
				return nil
			}
			res, err := eph.Push(cmd.Context(), queue, buf, opts)
			if err != nil {
				return ephemeralFail(err, "push")
			}
			pushed += res.Pushed
			buf = buf[:0]
			return nil
		}

		scanner := bufio.NewScanner(src)
		scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
		lines := 0
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			var payload any
			if err := json.Unmarshal([]byte(line), &payload); err != nil {
				return clierr.Userf("invalid JSON line: %v", err)
			}
			lines++
			buf = append(buf, queen.EphemeralMessage{Payload: payload})
			if len(buf) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		if scerr := scanner.Err(); scerr != nil {
			if errors.Is(scerr, bufio.ErrTooLong) {
				return clierr.Userf("input line too long; pre-split with jq -c")
			}
			return clierr.Userf("read input: %v", scerr)
		}
		if err := flush(); err != nil {
			return err
		}

		if lines == 0 {
			return clierr.Empty("nothing pushed")
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "pushed=%d\n", pushed)
		}
		return nil
	},
}

// ---------------------------------------------------------------------------
// pop
// ---------------------------------------------------------------------------

var ephemeralPopCmd = &cobra.Command{
	Use:   "pop <queue>",
	Short: "Pop messages from an ephemeral queue",
	Long: `One-shot pop, printed as NDJSON on stdout: {id, partition, attempts,
payload} per line.

  queenctl ephemeral pop inbox --wait --timeout 5s
  queenctl ephemeral pop presence --cg workers -n 50 | jq -s length

--wait is a real long poll parked on a RAM gate with no database behind it and
no polling interval anywhere, which is why an ephemeral inbox answers in
transport time.

The id is OPAQUE and encodes the owning broker incarnation: that is what lets an
ack arriving after a restart answer "stale" rather than acking somebody else's
message. Feed it back to 'ephemeral ack', never parse it.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		batch, err := c.Q.Ephemeral().Pop(cmd.Context(), args[0], queen.EphemeralPopOptions{
			Partition:     ephPopPartition,
			Batch:         ephPopBatch,
			Wait:          ephPopWait,
			TimeoutMillis: int(ephPopTimeout.Milliseconds()),
			Group:         ephPopGroup,
			AutoAck:       ephPopAutoAck,
		})
		if err != nil {
			return ephemeralFail(err, "pop")
		}
		if len(batch.Messages) == 0 {
			return clierr.Empty("no messages")
		}

		enc := json.NewEncoder(stdout())
		for _, msg := range batch.Messages {
			if err := enc.Encode(map[string]any{
				"queue":     batch.Queue,
				"id":        msg.ID,
				"partition": msg.Partition,
				"attempts":  msg.Attempts,
				"payload":   json.RawMessage(msg.Payload),
			}); err != nil {
				return clierr.Userf("encode: %v", err)
			}
		}
		return nil
	},
}

// ---------------------------------------------------------------------------
// ack
// ---------------------------------------------------------------------------

var ephemeralAckCmd = &cobra.Command{
	Use:   "ack <queue> [id...]",
	Short: "Acknowledge popped ephemeral messages",
	Long: `Acknowledge one or more message ids, read from the arguments or, when
none are given, one per line on stdin.

  queenctl ephemeral ack inbox e:9f1:Default:12 --cg workers
  queenctl ephemeral pop inbox --cg workers | jq -r .id | queenctl ephemeral ack inbox --cg workers

Pass the same --cg the pop used: cursors are per group.

The outcome is printed per id and is NOT an error taxonomy. "stale" means the id
belongs to a previous incarnation of the ring — a restart, i.e. the loss
contract rather than a bug — and "unknown" means the lease is no longer ours to
release. Both are information.

--status is completed (the default), failed or retry. A failed or retried
message comes back with attempts+1 until the queue's retry limit, after which it
is dropped and counted. There is no DLQ on this class.`,
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		queue := args[0]
		ids := args[1:]
		if len(ids) == 0 {
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				if line := strings.TrimSpace(scanner.Text()); line != "" {
					ids = append(ids, line)
				}
			}
			if err := scanner.Err(); err != nil {
				return clierr.Userf("read input: %v", err)
			}
		}
		if len(ids) == 0 {
			return clierr.Empty("no ids to acknowledge")
		}

		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		results, err := c.Q.Ephemeral().Ack(cmd.Context(), queue, ids, queen.EphemeralAckOptions{
			Group:  ephAckGroup,
			Status: ephAckStatus,
			Error:  ephAckError,
		})
		if err != nil {
			return ephemeralFail(err, "ack")
		}
		if !quiet() {
			for _, r := range results {
				fmt.Fprintf(stdout(), "%s\t%s\n", r.ID, r.Outcome)
			}
		}
		return nil
	},
}

// ---------------------------------------------------------------------------
// configure / reset / delete
// ---------------------------------------------------------------------------

var ephemeralConfigureCmd = &cobra.Command{
	Use:   "configure <queue>",
	Short: "Declare an ephemeral queue and its bounds",
	Long: `Declare a queue: the OPTIONS are persisted in PostgreSQL, so the
configuration survives a restart and the queue comes back declared and EMPTY.

Optional in every sense — a push or a pop that names an unknown queue creates it
implicitly with the tenant defaults. Declare when you want non-default bounds,
or when you want the queue to exist in the dashboard before its first message.

  queenctl ephemeral configure presence --max-length 5000 --policy dropOldest
  queenctl ephemeral configure inbox --ttl-seconds 30 --lease-seconds 10

--ttl-seconds is NOT the durable queue's retention: retention cleans consumed
history and never touches pending, while this drops UNCONSUMED messages. Only
the flags actually given travel; the rest stay the broker's defaults.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		var opts queen.EphemeralOptions
		flags := cmd.Flags()
		if flags.Changed("max-bytes") {
			opts.MaxBytes = queen.Int64(ephCfgMaxBytes)
		}
		if flags.Changed("max-length") {
			opts.MaxLength = queen.Int64(ephCfgMaxLength)
		}
		if flags.Changed("policy") {
			opts.Policy = ephCfgPolicy
		}
		if flags.Changed("ttl-seconds") {
			opts.TTLSeconds = queen.Int64(ephCfgTTLSeconds)
		}
		if flags.Changed("lease-seconds") {
			opts.LeaseSeconds = queen.Int64(ephCfgLeaseSeconds)
		}
		if flags.Changed("retry-limit") {
			opts.RetryLimit = queen.Int64(ephCfgRetryLimit)
		}
		if flags.Changed("window-ms") || flags.Changed("window-count") {
			opts.WindowBuffer = &queen.EphemeralWindowBuffer{Ms: ephCfgWindowMs, Count: ephCfgWindowCount}
		}

		data, err := c.Q.Ephemeral().Configure(cmd.Context(), args[0], opts)
		if err != nil {
			return ephemeralFail(err, "configure")
		}
		if quiet() {
			return nil
		}
		return renderEphemeral(data)
	},
}

var ephemeralResetCmd = &cobra.Command{
	Use:   "reset <queue>",
	Short: "Drop every message, void every lease, rewind every group cursor",
	Long: `Reset destroys the queue's contents and rewinds all of its cursors.
The declared configuration stays.

It is only defensible because of the loss contract: the class never promised to
keep any of this. Everything an in-flight consumer holds a lease on is gone, and
its acks will answer "stale".`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if !ephResetYes {
			return clierr.Userf("refusing to drop the contents of %q without --yes", args[0])
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		dropped, err := c.Q.Ephemeral().Reset(cmd.Context(), args[0])
		if err != nil {
			return ephemeralFail(err, "reset")
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "dropped=%d\n", dropped)
		}
		return nil
	},
}

var ephemeralDeleteCmd = &cobra.Command{
	Use:     "delete <queue>",
	Aliases: []string{"rm"},
	Short:   "Delete an ephemeral queue: contents, cursors and declared config",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if !ephDeleteYes {
			return clierr.Userf("refusing to delete %q without --yes", args[0])
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		deleted, err := c.Q.Ephemeral().Delete(cmd.Context(), args[0])
		if err != nil {
			return ephemeralFail(err, "delete")
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "queue=%s deleted=%t declared=%t\n", deleted.Queue, deleted.Deleted, deleted.Declared)
		}
		// A queue that was not there is a 200 with deleted:false, never a 404.
		// Reporting that as success is the in-house scar this check exists for.
		if !deleted.Deleted {
			return clierr.Empty("nothing to delete")
		}
		return nil
	},
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

var ephemeralQueuesCmd = &cobra.Command{
	Use:     "queues",
	Aliases: []string{"list", "ls"},
	Short:   "List this tenant's ephemeral queues, declared and implicit",
	Long: `List the live ephemeral queues with their gauges.

Free to poll: the numbers are read out of the broker's own memory, with no
database behind them — unlike the durable meter, whose 1s poll is load-bearing
on PostgreSQL.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		data, err := c.Q.Ephemeral().Queues(cmd.Context())
		if err != nil {
			return ephemeralFail(err, "queues")
		}
		return renderEphemeral(data)
	},
}

var ephemeralDepthCmd = &cobra.Command{
	Use:   "depth <queue>",
	Short: "Show the gauges of one ephemeral queue",
	Long: `Depth for one queue: pending, bytes, per-partition and per-group rows.

This is the one verb of the family that answers "no such queue" — every other
one either creates the queue by naming it or describes a miss inside a 200. It
exits 4 in that case, because an implicit ephemeral queue IS its ring: a name
with no ring behind it has either never been used or has been idle-collected.`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()

		data, err := c.Q.Ephemeral().Depth(cmd.Context(), args[0])
		if err != nil {
			return ephemeralFail(err, "depth")
		}
		return renderEphemeral(data)
	},
}

// renderEphemeral prints a status body. The shape is the broker's, so it is
// rendered as a document rather than forced into columns this CLI would have to
// invent — the same choice `cg describe` makes.
func renderEphemeral(data map[string]any) error {
	r, err := rendererFor(output.View{}, stdout())
	if err != nil {
		return err
	}
	if r.Format == output.FormatTable {
		r.Format = output.FormatYAML
	}
	return r.Render(data)
}

func init() {
	ephemeralPushCmd.Flags().StringVar(&ephPushPartition, "partition", "", "ring to push into (omitted when unset: the broker picks)")
	ephemeralPushCmd.Flags().StringVar(&ephPushData, "data", "", "single inline JSON payload (skips stdin)")
	ephemeralPushCmd.Flags().StringVarP(&ephPushFile, "file", "f", "", "read NDJSON from file instead of stdin")
	ephemeralPushCmd.Flags().IntVar(&ephPushBatch, "batch", 100, "messages per HTTP request")

	ephemeralPopCmd.Flags().StringVar(&ephPopGroup, "cg", "", "consumer group: same group competes, its own fans out, none is queue mode")
	ephemeralPopCmd.Flags().StringVar(&ephPopPartition, "partition", "", "specific partition")
	ephemeralPopCmd.Flags().IntVarP(&ephPopBatch, "batch", "n", 1, "maximum messages to return")
	ephemeralPopCmd.Flags().BoolVar(&ephPopWait, "wait", false, "long-poll until messages arrive or timeout")
	ephemeralPopCmd.Flags().DurationVar(&ephPopTimeout, "timeout", 10*time.Second, "long-poll timeout (only sent with --wait)")
	ephemeralPopCmd.Flags().BoolVar(&ephPopAutoAck, "auto-ack", false, "commit at delivery (at-most-once; nothing to ack afterwards)")

	ephemeralAckCmd.Flags().StringVar(&ephAckGroup, "cg", "", "consumer group the pop used")
	ephemeralAckCmd.Flags().StringVar(&ephAckStatus, "status", "", "completed|failed|retry (default completed)")
	ephemeralAckCmd.Flags().StringVar(&ephAckError, "error", "", "failure reason (accepted and ignored on this class: there is no DLQ row to record it on)")

	ephemeralConfigureCmd.Flags().Int64Var(&ephCfgMaxBytes, "max-bytes", 0, "per-queue byte budget")
	ephemeralConfigureCmd.Flags().Int64Var(&ephCfgMaxLength, "max-length", 0, "per-queue message budget")
	ephemeralConfigureCmd.Flags().StringVar(&ephCfgPolicy, "policy", "", "reject|dropOldest — what breaching the budget does")
	ephemeralConfigureCmd.Flags().Int64Var(&ephCfgTTLSeconds, "ttl-seconds", 0, "drop messages older than this (NOT the durable retention)")
	ephemeralConfigureCmd.Flags().Int64Var(&ephCfgLeaseSeconds, "lease-seconds", 0, "redelivery lease")
	ephemeralConfigureCmd.Flags().Int64Var(&ephCfgRetryLimit, "retry-limit", 0, "attempts before a message is dropped and counted")
	ephemeralConfigureCmd.Flags().IntVar(&ephCfgWindowMs, "window-ms", 0, "let a waiting pop fatten its batch for this long")
	ephemeralConfigureCmd.Flags().IntVar(&ephCfgWindowCount, "window-count", 0, "batch size a waiting pop fattens towards")

	ephemeralResetCmd.Flags().BoolVar(&ephResetYes, "yes", false, "confirm: the contents are destroyed")
	ephemeralDeleteCmd.Flags().BoolVar(&ephDeleteYes, "yes", false, "confirm: the queue and its declared config are destroyed")

	ephemeralCmd.AddCommand(
		ephemeralPushCmd,
		ephemeralPopCmd,
		ephemeralAckCmd,
		ephemeralConfigureCmd,
		ephemeralResetCmd,
		ephemeralDeleteCmd,
		ephemeralQueuesCmd,
		ephemeralDepthCmd,
	)
	rootCmd.AddCommand(ephemeralCmd)
}
