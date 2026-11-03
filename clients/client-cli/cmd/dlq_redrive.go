package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"

	clierr "github.com/smartpricing/queen/clients/client-cli/v2/internal/errors"
	"github.com/smartpricing/queen/clients/client-cli/v2/internal/output"
	queen "github.com/smartpricing/queen/clients/client-go/v2"
	"github.com/spf13/cobra"
)

// `dlq redrive` replays every dead-letter row the filters match back onto its
// queue: 'dlq retry' for many rows (SQS calls this a redrive), decided in
// queen-mq/queen#147. It reads every match first, then replays each row by its
// row id, POST /api/v1/dlq/{id}/replay, the route the console's Replay uses:
// that route moves THAT row (lock, push, delete, one transaction, transaction
// id dlq:<row id>), where the address route behind 'dlq retry' picks the
// newest snapshot of the address, which may be another consumer group's row.

// refuse to start on more rows than this without --max
const redriveDefaultMax = 10_000

// rows per list request (a var so the tests can page with a few rows)
var redrivePage = 500

var (
	redriveMax      int
	redriveParallel int
)

var dlqRedriveCmd = &cobra.Command{
	Use:   "redrive",
	Short: "Replay every DLQ message matching filters back onto its queue, oldest first",
	Long: `Reads every dead-letter row that --queue/--cg/--partition/--from/--to
match, then replays each one back onto its queue and partition, as
'dlq retry' does for one message. SQS calls this a redrive.

The order: each partition's messages go back in the order they were first
written (their offset), one at a time, so the replays of a partition keep
their order among themselves. Different partitions run side by side
(--parallel, 4 by default).

Read this before running it:

  - A replayed message lands at the TAIL of its partition, after everything
    written since it failed, so it is out of order with its own key's newer
    messages, and its age clock restarts. Only the replays keep their order.
  - It is safe to run again. Each replay carries the transaction id
    'dlq:<row id>' and removes its row in the same transaction, so a second
    run replays only what the first one left. A row replayed or purged in the
    meantime answers 404 and counts as gone.
  - A message dead-lettered by more than one consumer group has a row per
    group, and one replay reaches every group. Without --cg the command
    refuses to start on such messages: name the group with --cg.
  - result "duplicate" means nothing was written and the row was kept: the
    destination already carries that transaction id.
  - It stops sending at the first answer that says the next one fails the
    same way (401, 403, 429, or no broker).

Pass --dry-run to print the plan without sending anything. It refuses to
start on more than --max rows (10,000 by default).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if !dlqYes && !dlqDryRun {
			return clierr.Userf("refusing to redrive without --yes: it appends messages and removes their dead-letter records (use --dry-run for a preview)")
		}
		if redriveMax < 1 {
			return clierr.Userf("--max must be at least 1")
		}
		c, cleanup, err := newClient()
		if err != nil {
			return err
		}
		defer cleanup()
		params, err := dlqParams()
		if err != nil {
			return err
		}
		ctx := cmd.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		rows, err := readRedriveRows(ctx, c.A, params, redriveMax)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return clierr.Empty("no DLQ messages matched")
		}
		if len(rows) > redriveMax {
			return clierr.Userf("more than %d dead-letter rows match: narrow the filters, or raise --max", redriveMax)
		}
		if dlqCG == "" {
			if shared := sharedAddresses(rows); len(shared) > 0 {
				return clierr.Userf("%s", sharedMessage(shared))
			}
		}
		plan := planRedrive(rows)

		if dlqDryRun {
			for _, p := range plan {
				for _, r := range p.rows {
					fmt.Fprintf(stdout(), "[dry-run] replay %s/%s offset %s %s (group %s)\n", r.Queue, r.Partition, offsetText(r), r.TransactionID, r.ConsumerGroup)
				}
			}
			if !quiet() {
				fmt.Fprintf(stdout(), "rows=%d partitions=%d\n", len(rows), len(plan))
			}
			return nil
		}

		parallel := redriveParallel
		if parallel < 1 {
			parallel = 1
		}
		sum := runRedrive(ctx, plan, parallel, func(ctx context.Context, id string) (redriveVerdict, error) {
			return replayRow(ctx, c.Q, id)
		})
		for _, line := range sum.problems {
			fmt.Fprintln(stdout(), line)
		}
		if !quiet() {
			fmt.Fprintf(stdout(), "replayed=%d duplicate=%d gone=%d failed=%d not-sent=%d\n", sum.moved, sum.duplicate, sum.gone, sum.failed, sum.notSent)
		}
		if sum.failed > 0 || sum.notSent > 0 {
			return clierr.Server(fmt.Errorf("%d of %d rows were not replayed: re-run the same command once the cause is fixed, it replays only what is left", sum.failed+sum.notSent, len(rows)))
		}
		return nil
	},
}

// redriveRow is what the plan needs of a dead-letter row.
type redriveRow struct {
	ID            string
	PartitionID   string
	Partition     string
	Queue         string
	TransactionID string
	ConsumerGroup string
	Offset        float64
	HasOffset     bool
	FailedAt      string
}

func rowOf(m map[string]any) (redriveRow, bool) {
	s := func(k string) string { v, _ := m[k].(string); return v }
	r := redriveRow{
		ID:            s("id"),
		PartitionID:   s("partitionId"),
		Partition:     s("partition"),
		Queue:         s("queue"),
		TransactionID: s("transactionId"),
		ConsumerGroup: s("consumerGroup"),
		FailedAt:      s("failedAt"),
	}
	if r.Queue == "" {
		r.Queue = s("queueName")
	}
	if v, ok := m["offset"].(float64); ok {
		r.Offset, r.HasOffset = v, true
	}
	return r, r.ID != "" && r.PartitionID != ""
}

func offsetText(r redriveRow) string {
	if !r.HasOffset {
		return "?"
	}
	return fmt.Sprintf("%.0f", r.Offset)
}

// readRedriveRows pages through the list until it is exhausted or holds more
// than max rows (one more than max is enough to refuse). Rows are kept once by
// row id: a dead letter that arrives while the list is read shifts the pages,
// and the shifted row comes back on the next one.
func readRedriveRows(ctx context.Context, a *queen.Admin, params queen.ListDLQParams, max int) ([]redriveRow, error) {
	params.Limit = redrivePage
	params.Offset = 0
	seen := map[string]bool{}
	var out []redriveRow
	for {
		data, err := a.ListDLQ(ctx, params)
		if err != nil {
			return nil, clierr.Server(err)
		}
		page := output.AsArray(data, "messages")
		if page == nil {
			page = output.AsArray(data, "data")
		}
		for _, raw := range page {
			m := output.AsMap(raw)
			if m == nil {
				continue
			}
			r, ok := rowOf(m)
			if !ok || seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			out = append(out, r)
		}
		if len(page) < redrivePage || len(out) > max {
			return out, nil
		}
		params.Offset += redrivePage
	}
}

// One partition's rows, in the order they go back.
type redrivePartition struct {
	key  string
	rows []redriveRow
}

// planRedrive groups the rows by partition, each partition oldest first: by
// offset, the order the messages were first written; a row without one goes
// after those with one, by failure time. Partitions are listed by queue and
// partition name, so a dry run reads the same every time.
func planRedrive(rows []redriveRow) []redrivePartition {
	by := map[string]*redrivePartition{}
	var order []*redrivePartition
	for _, r := range rows {
		p := by[r.PartitionID]
		if p == nil {
			p = &redrivePartition{key: r.Queue + "\x1f" + r.Partition + "\x1f" + r.PartitionID}
			by[r.PartitionID] = p
			order = append(order, p)
		}
		p.rows = append(p.rows, r)
	}
	for _, p := range order {
		sort.SliceStable(p.rows, func(i, j int) bool {
			a, b := p.rows[i], p.rows[j]
			if a.HasOffset != b.HasOffset {
				return a.HasOffset
			}
			if a.HasOffset && a.Offset != b.Offset {
				return a.Offset < b.Offset
			}
			if a.FailedAt != b.FailedAt {
				return a.FailedAt < b.FailedAt
			}
			return a.ID < b.ID
		})
	}
	sort.SliceStable(order, func(i, j int) bool { return order[i].key < order[j].key })
	out := make([]redrivePartition, len(order))
	for i, p := range order {
		out[i] = *p
	}
	return out
}

// sharedAddresses lists the messages (partition + transaction id) that more
// than one consumer group dead-lettered, as "queue/partition tx (a, b)".
func sharedAddresses(rows []redriveRow) []string {
	groups := map[string]map[string]bool{}
	label := map[string]string{}
	for _, r := range rows {
		k := r.PartitionID + "\x1f" + r.TransactionID
		if groups[k] == nil {
			groups[k] = map[string]bool{}
			label[k] = fmt.Sprintf("%s/%s %s", r.Queue, r.Partition, r.TransactionID)
		}
		groups[k][r.ConsumerGroup] = true
	}
	var out []string
	for k, g := range groups {
		if len(g) < 2 {
			continue
		}
		names := make([]string, 0, len(g))
		for n := range g {
			names = append(names, n)
		}
		sort.Strings(names)
		out = append(out, fmt.Sprintf("%s (%s)", label[k], strings.Join(names, ", ")))
	}
	sort.Strings(out)
	return out
}

func sharedMessage(shared []string) string {
	shown := shared
	if len(shown) > 3 {
		shown = shown[:3]
	}
	more := ""
	if len(shared) > len(shown) {
		more = fmt.Sprintf(" and %d more", len(shared)-len(shown))
	}
	were := "messages were"
	if len(shared) == 1 {
		were = "message was"
	}
	return fmt.Sprintf("%d %s dead-lettered by more than one consumer group, and one replay reaches every group: name the group with --cg (%s%s)",
		len(shared), were, strings.Join(shown, "; "), more)
}

// What the broker said about one row.
type redriveVerdict int

const (
	verdictMoved redriveVerdict = iota
	verdictDuplicate
	verdictGone
)

// errStop is an answer that says the next call fails the same way.
type errStop struct{ err error }

func (e errStop) Error() string { return e.err.Error() }
func (e errStop) Unwrap() error { return e.err }

// replayRow moves one dead-letter row by its id. 200 moved or duplicate; 404
// with result "gone" when the row was replayed or purged meanwhile. A 404
// without that body is a broker that does not have the route: stop.
func replayRow(ctx context.Context, q *queen.Queen, id string) (redriveVerdict, error) {
	data, err := q.GetHttpClient().Post(ctx, "/api/v1/dlq/"+url.PathEscape(id)+"/replay", map[string]any{}, queen.WithoutFailoverRetry())
	if err != nil {
		var he *queen.HTTPError
		if errors.As(err, &he) {
			switch {
			case he.StatusCode == 404 && strings.Contains(he.Body, `"gone"`):
				return verdictGone, nil
			case he.StatusCode == 404:
				return 0, errStop{fmt.Errorf("this broker has no POST /api/v1/dlq/{id}/replay: %w", err)}
			case he.StatusCode == 401 || he.StatusCode == 403 || he.StatusCode == 429:
				return 0, errStop{err}
			}
			return 0, err
		}
		// No HTTP answer at all: the broker is not reachable.
		return 0, errStop{err}
	}
	if result, _ := data["result"].(string); result == "duplicate" {
		return verdictDuplicate, nil
	}
	if removed, ok := data["dlqRowRemoved"].(bool); ok && !removed {
		return verdictDuplicate, nil
	}
	return verdictMoved, nil
}

type redriveSummary struct {
	moved, duplicate, gone, failed, notSent int
	problems                                []string
}

// runRedrive replays the plan: partitions side by side, `parallel` at a time,
// each partition's rows one after the other in plan order. After a stop, no
// new row is sent; the rows not sent are counted.
func runRedrive(ctx context.Context, plan []redrivePartition, parallel int, replay func(context.Context, string) (redriveVerdict, error)) redriveSummary {
	var (
		mu      sync.Mutex
		sum     redriveSummary
		stopped bool
		wg      sync.WaitGroup
	)
	work := make(chan redrivePartition)
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range work {
				for n, r := range p.rows {
					mu.Lock()
					halt := stopped
					mu.Unlock()
					if halt {
						mu.Lock()
						sum.notSent += len(p.rows) - n
						mu.Unlock()
						break
					}
					v, err := replay(ctx, r.ID)
					mu.Lock()
					switch {
					case err != nil:
						sum.failed++
						var stop errStop
						if errors.As(err, &stop) && !stopped {
							stopped = true
							sum.problems = append(sum.problems, fmt.Sprintf("stopped: %v", err))
						} else if !errors.As(err, &stop) {
							sum.problems = append(sum.problems, fmt.Sprintf("error: %s/%s offset %s %s: %v", r.Queue, r.Partition, offsetText(r), r.TransactionID, err))
						}
					case v == verdictDuplicate:
						sum.duplicate++
						sum.problems = append(sum.problems, fmt.Sprintf("duplicate: %s/%s offset %s %s: nothing written, the row was kept (the destination already carries dlq:%s)", r.Queue, r.Partition, offsetText(r), r.TransactionID, r.ID))
					case v == verdictGone:
						sum.gone++
					default:
						sum.moved++
					}
					// A partition stops at its first failure: the rows after it
					// would land before the one that failed.
					failedHere := err != nil
					mu.Unlock()
					if failedHere {
						mu.Lock()
						sum.notSent += len(p.rows) - n - 1
						mu.Unlock()
						break
					}
				}
			}
		}()
	}
	for _, p := range plan {
		work <- p
	}
	close(work)
	wg.Wait()
	return sum
}

func init() {
	c := dlqRedriveCmd
	c.Flags().StringVar(&dlqQueue, "queue", "", "filter by queue")
	c.Flags().StringVar(&dlqCG, "cg", "", "filter by consumer group")
	c.Flags().StringVar(&dlqPartition, "partition", "", "filter by partition")
	c.Flags().StringVar(&dlqFrom, "from", "", "filter from time")
	c.Flags().StringVar(&dlqTo, "to", "", "filter to time")
	c.Flags().BoolVar(&dlqDryRun, "dry-run", false, "print the plan without sending")
	c.Flags().BoolVar(&dlqYes, "yes", false, "confirm: it appends messages and removes their dead-letter records")
	c.Flags().IntVar(&redriveMax, "max", redriveDefaultMax, "refuse to start when more rows than this match")
	c.Flags().IntVar(&redriveParallel, "parallel", 4, "partitions replayed side by side (each partition stays in order)")
	dlqCmd.AddCommand(c)
}
