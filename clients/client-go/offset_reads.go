package queen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Reads by offset and partition discovery, as two queue methods:
//
//	read, err := q.Queue("orders").Fetch(ctx, []string{"customer-42", "customer-7"})
//	read["customer-42"].Records // offset, transactionId, payload, ts
//
//	changed, err := q.Queue("orders").ChangedSince(ctx, since)
//	changed.Partitions, changed.SafeTime
//
// Fetch is POST /api/v1/fetch and ChangedSince is POST /api/v1/partitions/changed.
// Neither takes a lease, moves a cursor or needs an ack: two readers asking
// the same question get the same answer, and no consumer group notices them.
// Each is served from the answering node's own applied state.
//
// Both do the loops a caller of the raw routes has to write:
//   - Fetch asks for at most 1000 partitions per call, and asks again from one
//     past the last record until each partition reaches the high watermark its
//     first answer reported. A partition whose start was deleted by retention
//     is read from its oldest kept record (an offset below logStartOffset
//     answers OFFSET_OUT_OF_RANGE and no records, so a loop that does not
//     restart there silently reads nothing).
//   - ChangedSince follows the page cursor (next) until the broker says the
//     pass is over, and answers the smallest safeTime of its pages.
//
// Both decode the raw body into typed structs, so an offset is an int64 all
// the way (the map path of this client turns every number into a float64).

const (
	// The broker refuses more than 1024 entries in one call; 1000 leaves room.
	fetchEntriesPerCall = 1000
	// DefaultFetchMaxBytes is the payload bytes one fetch call may carry: each
	// entry's budget and the ceiling of the whole call. 8 MiB is the most the
	// broker reads for one entry.
	DefaultFetchMaxBytes = 8 * 1024 * 1024
	// Partitions per discovery page: the broker's default and its ceiling.
	changedPage = 1000

	errOffsetOutOfRange        = "OFFSET_OUT_OF_RANGE"
	errUnknownTopicOrPartition = "UNKNOWN_TOPIC_OR_PARTITION"
)

// FetchOptions tunes QueueBuilder.Fetch.
type FetchOptions struct {
	// Offsets starts a partition at an offset, e.g. the NextOffset of an
	// earlier read. A partition not named here starts at 0, the oldest record
	// still kept.
	Offsets map[string]int64
	// MaxBytes caps the payload bytes of one call. 0 = DefaultFetchMaxBytes.
	MaxBytes int
}

// FetchedRecord is one record read by offset.
type FetchedRecord struct {
	Offset int64 `json:"offset"`
	// TransactionID is the id the message was pushed with.
	TransactionID string `json:"transactionId"`
	// Payload is the payload as pushed (null when it was empty).
	Payload json.RawMessage `json:"payload"`
	// Ts is when the push that wrote the record was stored.
	Ts time.Time `json:"ts"`
}

// FetchedPartition is what Fetch read of one partition.
type FetchedPartition struct {
	Partition string          `json:"partition"`
	Records   []FetchedRecord `json:"records"`
	// HighWatermark is the next offset the partition will assign.
	HighWatermark int64 `json:"highWatermark"`
	// LogStartOffset is the oldest offset retention kept.
	LogStartOffset int64 `json:"logStartOffset"`
	// NextOffset is where to start next time to read only what is new. Above
	// HighWatermark it says the partition was deleted and written again.
	NextOffset int64 `json:"nextOffset"`
}

// ChangedPartition is one partition of a discovery answer.
type ChangedPartition struct {
	Name string `json:"name"`
	// ID is the partition's id, new when a partition is deleted and created
	// again under the same name.
	ID string `json:"id"`
	// LastOffset is the offset of its last record (-1 when none).
	LastOffset  int64     `json:"lastOffset"`
	LogStart    int64     `json:"logStart"`
	LastWriteAt time.Time `json:"lastWriteAt"`
}

// ChangedPartitions is what ChangedSince answers.
type ChangedPartitions struct {
	Partitions []ChangedPartition
	// SafeTime is the broker's time up to which every write is counted in the
	// answer: pass it as the next since to see only what changed after this
	// call. It comes from the broker's clock; never compare it with the local
	// one.
	SafeTime time.Time
}

type fetchEntryWire struct {
	Queue     string `json:"queue"`
	Partition string `json:"partition"`
	Offset    int64  `json:"offset"`
	MaxBytes  int    `json:"maxBytes"`
}

type fetchAnswerWire struct {
	Entries []struct {
		Partition      string          `json:"partition"`
		Records        []FetchedRecord `json:"records"`
		HighWatermark  *int64          `json:"highWatermark"`
		LogStartOffset *int64          `json:"logStartOffset"`
		Error          string          `json:"error"`
	} `json:"entries"`
}

type changedAnswerWire struct {
	SafeTime *time.Time `json:"safeTime"`
	Entries  []struct {
		Partitions []ChangedPartition `json:"partitions"`
		Next       *string            `json:"next"`
		Error      string             `json:"error"`
	} `json:"entries"`
}

// Every call of one read goes to the same node: a follower answers from what
// it has applied, so two nodes can disagree about where a partition ends.
func readAffinity(queue string) string { return queue + ":*:__READ__" }

func (qb *QueueBuilder) postRead(ctx context.Context, path string, body interface{}, out interface{}) error {
	raw, err := qb.queen.httpClient.doRequestRaw(ctx, http.MethodPost, path, body, 0, readAffinity(qb.queueName), resolveRequestOptions(nil))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%s: cannot read the answer: %w", path, err)
	}
	return nil
}

// Fetch answers every record of these partitions, read by offset: from each
// partition's offset in opts.Offsets (default 0, the oldest record still kept)
// up to where the partition ended when it was first read. No lease, no cursor,
// nothing to ack.
//
// The map has one FetchedPartition per partition named. A partition nobody
// has written, or of a queue that does not exist, has no records. So does an
// offset past the end of the partition.
func (qb *QueueBuilder) Fetch(ctx context.Context, partitions []string, opts ...FetchOptions) (map[string]FetchedPartition, error) {
	if qb.queueName == "" {
		return nil, errors.New("queue name is required for fetch")
	}
	var opt FetchOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	maxBytes := opt.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultFetchMaxBytes
	}
	if maxBytes < 0 {
		return nil, fmt.Errorf("fetch: MaxBytes must be positive, got %d", maxBytes)
	}

	type partState struct {
		FetchedPartition
		end    int64
		hasEnd bool
	}
	names := make([]string, 0, len(partitions))
	state := make(map[string]*partState, len(partitions))
	for _, name := range partitions {
		if name == "" {
			return nil, errors.New("fetch: a partition name must be a non-empty string")
		}
		if _, seen := state[name]; seen {
			continue
		}
		start := opt.Offsets[name]
		if start < 0 {
			return nil, fmt.Errorf("fetch: Offsets[%q] must be non-negative, got %d", name, start)
		}
		names = append(names, name)
		state[name] = &partState{FetchedPartition: FetchedPartition{Partition: name, Records: []FetchedRecord{}, NextOffset: start}}
	}

	pending := names
	for len(pending) > 0 {
		var again []string
		for i := 0; i < len(pending); i += fetchEntriesPerCall {
			chunk := pending[i:min(i+fetchEntriesPerCall, len(pending))]
			entries := make([]fetchEntryWire, len(chunk))
			for j, name := range chunk {
				entries[j] = fetchEntryWire{Queue: qb.queueName, Partition: name, Offset: state[name].NextOffset, MaxBytes: maxBytes}
			}
			var answer fetchAnswerWire
			body := map[string]interface{}{"entries": entries, "maxBytes": maxBytes}
			if err := qb.postRead(ctx, "/api/v1/fetch", body, &answer); err != nil {
				return nil, err
			}
			if len(answer.Entries) != len(chunk) {
				return nil, fmt.Errorf("fetch %s: the broker answered %d entries for %d asked", qb.queueName, len(answer.Entries), len(chunk))
			}

			delivered := 0
			// Entries that came back empty while short of their end: the
			// call's byte ceiling was spent by the entries before them.
			var starved []string
			for j, name := range chunk {
				entry := answer.Entries[j]
				s := state[name]
				if entry.HighWatermark != nil {
					s.HighWatermark = *entry.HighWatermark
				}
				if entry.LogStartOffset != nil {
					s.LogStartOffset = *entry.LogStartOffset
				}
				if entry.Error != "" {
					if entry.Error == errOffsetOutOfRange && s.NextOffset < s.LogStartOffset {
						// Retention deleted what was asked for: go on from the
						// oldest kept.
						s.NextOffset = s.LogStartOffset
						if !s.hasEnd {
							s.end, s.hasEnd = s.HighWatermark, true
						}
						if s.NextOffset < s.end {
							again = append(again, name)
						}
						continue
					}
					if entry.Error == errOffsetOutOfRange || entry.Error == errUnknownTopicOrPartition {
						continue
					}
					return nil, fmt.Errorf("fetch %s/%s: the broker answered %s", qb.queueName, name, entry.Error)
				}
				if !s.hasEnd {
					s.end, s.hasEnd = s.HighWatermark, true
				}
				if n := len(entry.Records); n > 0 {
					delivered += n
					s.Records = append(s.Records, entry.Records...)
					s.NextOffset = entry.Records[n-1].Offset + 1
					if s.NextOffset < s.end {
						again = append(again, name)
					}
				} else if s.NextOffset < s.end {
					starved = append(starved, name)
				}
			}
			// An empty entry is asked again only when its call read something:
			// a call that read nothing at all would answer the same next time.
			if delivered > 0 {
				again = append(again, starved...)
			}
		}
		pending = again
	}

	out := make(map[string]FetchedPartition, len(names))
	for _, name := range names {
		out[name] = state[name].FetchedPartition
	}
	return out, nil
}

// ChangedSince answers the partitions of the queue written at or after since,
// in the order they were created, and the SafeTime of the answer. A zero since
// lists every partition of the queue. A queue that does not exist has none.
//
// Pass SafeTime as the next since to see only what changed after this call.
func (qb *QueueBuilder) ChangedSince(ctx context.Context, since time.Time) (ChangedPartitions, error) {
	out := ChangedPartitions{Partitions: []ChangedPartition{}}
	if qb.queueName == "" {
		return out, errors.New("queue name is required for ChangedSince")
	}
	var after *string
	for {
		entry := map[string]interface{}{"queue": qb.queueName, "limit": changedPage}
		if !since.IsZero() {
			entry["since"] = since.UTC().Format("2006-01-02T15:04:05.000000Z07:00")
		}
		if after != nil {
			entry["after"] = *after
		}
		var answer changedAnswerWire
		if err := qb.postRead(ctx, "/api/v1/partitions/changed", map[string]interface{}{"entries": []interface{}{entry}}, &answer); err != nil {
			return out, err
		}
		// Pages served by different nodes: the smallest is the one that holds
		// for all of them.
		if answer.SafeTime != nil && (out.SafeTime.IsZero() || answer.SafeTime.Before(out.SafeTime)) {
			out.SafeTime = *answer.SafeTime
		}
		if len(answer.Entries) == 0 {
			return out, fmt.Errorf("ChangedSince %s: the broker answered no entry", qb.queueName)
		}
		result := answer.Entries[0]
		if result.Error == errUnknownTopicOrPartition {
			return out, nil
		}
		if result.Error != "" {
			return out, fmt.Errorf("ChangedSince %s: the broker answered %s", qb.queueName, result.Error)
		}
		out.Partitions = append(out.Partitions, result.Partitions...)
		if result.Next == nil || *result.Next == "" {
			return out, nil
		}
		after = result.Next
	}
}
