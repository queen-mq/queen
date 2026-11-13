package queen

// Routing without copies (PLAN_ROUTING.md), all of it on a cluster at
// catalogue version 8 (an older cluster refuses each part with
// `cluster_version`, an older broker with a 400):
//
//   - headers, a map of strings on every message: PushItem.Headers and
//     PushBuilder.Headers on the way in; Message.Headers, FetchedRecord.Headers
//     and FeedEntry.Headers on the way out;
//   - filtered consumer groups: QueueBuilder.Where and WhereExpr, sent as the
//     `where` of every pop of the group (Admin.SetConsumerGroupWhere changes
//     it);
//   - reading queues: QueueConfig.From, Where, PartitionBy and Start;
//   - link: TransactionQueueBuilder.Link, the message a worker holds handed to
//     another queue without its payload (and the feed streams' .To(), which
//     links an output that is its input unchanged).
//
// The broker evaluates every where and checks every header against its
// limits; this file holds what the client puts on the wire for them.

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// ErrWhereNeedsGroup is what Pop and Consume return, before any request, for a
// builder with a where and no Group: a where is a consumer group's setting
// (the broker refuses it in queue mode).
var ErrWhereNeedsGroup = errors.New("a where filters a consumer group: name the group with Group")

// ErrWhereNeedsQueue is what Pop and Consume return, before any request, for a
// builder with a where that pops by namespace or task: the broker takes a where
// only on a pop of a named queue, and a discovery pop would drop it unread.
var ErrWhereNeedsQueue = errors.New("a where filters the pops of one named queue, not a namespace or task pop")

// ErrWhereUnsupported is what Pop and the consume loop return when a pop sent
// a where and the answer does not say it filtered. A broker that filters
// answers every such pop, an empty one too, with JSON that holds
// "filtered":true (never a bodiless 204); one older than filtered groups
// ignores the parameter and hands the group every message, which would look
// like a healthy consumer reading what it never asked for. So it stops, as a
// conflation the broker did not apply does (ErrConflationUnsupported). Match
// it with errors.Is.
var ErrWhereUnsupported = errors.New("this broker does not filter consumer groups; upgrade it (catalogue version 8)")

// ErrReadingQueuesUnsupported is what a configure with QueueConfig.From (or
// Option("from", ...)) returns when the answer holds no `route`: a broker
// older than reading queues stored the queue as an ordinary one.
var ErrReadingQueuesUnsupported = errors.New("this broker does not read other queues; upgrade it (catalogue version 8)")

// checkFilteredEcho inspects one pop answer of a pop that sent where. result
// is nil for a 204.
func checkFilteredEcho(result map[string]interface{}, where, queue, group string) error {
	if where == "" {
		return nil
	}
	if filtered, _ := result["filtered"].(bool); filtered {
		return nil
	}
	return fmt.Errorf("%w (queue %q, group %q)", ErrWhereUnsupported, queue, group)
}

// ---- headers -------------------------------------------------------------------

// headerBlock is headers as a frame carries them (frame flag 8):
//
//	u16 count | (u8 key_len | key | u16 value_len | value) x count
//
// sorted by name, little-endian, as the broker writes and checks it. nil for
// no headers: a frame without headers carries no block (the broker refuses an
// empty one). ok is false when they cannot be framed -- an empty name, a name
// over 255 bytes, a value or the block over 65,535 bytes, text that is not
// UTF-8 -- and the caller then sends the JSON body, whose answer says what the
// broker thinks of them (its own limits are far lower: 64 headers, 8 KiB).
func headerBlock(h map[string]string) ([]byte, bool) {
	if len(h) == 0 {
		return nil, true
	}
	keys := make([]string, 0, len(h))
	size := 2
	for k, v := range h {
		if k == "" || len(k) > 0xff || len(v) > 0xffff || !utf8.ValidString(k) || !utf8.ValidString(v) {
			return nil, false
		}
		keys = append(keys, k)
		size += 1 + len(k) + 2 + len(v)
	}
	if size > 0xffff {
		return nil, false
	}
	sort.Strings(keys)
	block := make([]byte, 0, size)
	block = binary.LittleEndian.AppendUint16(block, uint16(len(keys)))
	for _, k := range keys {
		v := h[k]
		block = append(block, byte(len(k)))
		block = append(block, k...)
		block = binary.LittleEndian.AppendUint16(block, uint16(len(v)))
		block = append(block, v...)
	}
	return block, true
}

// headersOfBlock reads a frame's headers block; ok false when it does not decode.
func headersOfBlock(b []byte) (map[string]string, bool) {
	r := &frameReader{b: b}
	n := r.u16()
	out := make(map[string]string, n)
	for i := 0; i < n && !r.bad; i++ {
		k := string(r.take(int(r.u8())))
		v := string(r.take(r.u16()))
		out[k] = v
	}
	if r.bad || r.p != len(b) {
		return nil, false
	}
	return out, true
}

// headersOf reads a message's `headers` from decoded JSON: nil when there are none.
func headersOf(v interface{}) map[string]string {
	m, ok := v.(map[string]interface{})
	if !ok || len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, x := range m {
		if s, ok := x.(string); ok {
			out[k] = s
		}
	}
	return out
}

// withHeaders is base with over written over it (over wins on a name both
// have); either alone is returned as it is, never copied.
func withHeaders(base, over map[string]string) map[string]string {
	if len(over) == 0 {
		return base
	}
	if len(base) == 0 {
		return over
	}
	out := make(map[string]string, len(base)+len(over))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range over {
		out[k] = v
	}
	return out
}

// ---- where -----------------------------------------------------------------------

// whereValue is a where as the broker reads it in a JSON body: the one-string
// form as it is ("data.kind in ('a', 'b') and data.total > 100"), or the
// conditions as a list of {field, op, value}. nil for none. Conditions are what
// feed.NormalizeWhere reads: a FeedCondition, a list of them, [field, op,
// value] lists, {field, op, value} maps.
func whereValue(where interface{}) (interface{}, error) {
	switch w := where.(type) {
	case nil:
		return nil, nil
	case string:
		if strings.TrimSpace(w) == "" {
			return nil, nil
		}
		return w, nil
	}
	conds, err := feed.NormalizeWhere(where)
	if err != nil {
		return nil, err
	}
	if len(conds) == 0 {
		return nil, nil
	}
	return conditionList(conds), nil
}

// conditionList is conditions as the broker's list, each {field, op, value}
// (no value for exists), written by Condition's own MarshalJSON: JSON text
// that a body embeds as it is and a pop sends as its `where` parameter.
func conditionList(conds []feed.Condition) json.RawMessage {
	out := []byte{'['}
	for i, c := range conds {
		if i > 0 {
			out = append(out, ',')
		}
		b, _ := c.MarshalJSON()
		out = append(out, b...)
	}
	return append(out, ']')
}

// ---- reading queues ------------------------------------------------------------

// ReadSource is one source of a reading queue (QueueConfig.From): a queue,
// and the where its messages must meet to be read.
type ReadSource struct {
	Queue string `json:"queue" yaml:"queue"`
	// Where is this source's own condition, written as QueueConfig.Where is.
	// nil: QueueConfig.Where applies to it (or nothing, and every message of
	// the source is read).
	Where interface{} `json:"where,omitempty" yaml:"where,omitempty"`
}

// ReadSources are a reading queue's sources. On the wire one source without a
// where of its own is its name, and anything else the list of names and
// {queue, where} objects. A JSON document or a manifest may write it either
// way too: `from: orders`, or a list.
type ReadSources []ReadSource

// ReadFrom is the queues named as sources, each read whole (or by
// QueueConfig.Where): QueueConfig{From: queen.ReadFrom("orders")}.
func ReadFrom(queues ...string) ReadSources {
	out := make(ReadSources, len(queues))
	for i, q := range queues {
		out[i] = ReadSource{Queue: q}
	}
	return out
}

// wire is the sources as the configure body carries them.
func (s ReadSources) wire() (interface{}, error) {
	for i, src := range s {
		if strings.TrimSpace(src.Queue) == "" {
			return nil, fmt.Errorf("from[%d]: a source names a queue", i)
		}
	}
	if len(s) == 1 && s[0].Where == nil {
		return s[0].Queue, nil
	}
	list := make([]interface{}, len(s))
	for i, src := range s {
		w, err := whereValue(src.Where)
		if err != nil {
			return nil, fmt.Errorf("from[%d] (%s): %w", i, src.Queue, err)
		}
		if w == nil {
			list[i] = src.Queue
		} else {
			list[i] = map[string]interface{}{"queue": src.Queue, "where": w}
		}
	}
	return list, nil
}

// MarshalJSON writes the sources as the broker reads them (see ReadSources).
func (s ReadSources) MarshalJSON() ([]byte, error) {
	w, err := s.wire()
	if err != nil {
		return nil, err
	}
	return json.Marshal(w)
}

// UnmarshalJSON reads a name, or a list of names and {queue, where} objects.
func (s *ReadSources) UnmarshalJSON(b []byte) error {
	v, err := jsv.Unmarshal(b)
	if err != nil {
		return err
	}
	return s.read(v)
}

// UnmarshalYAML reads the same shapes from a manifest (`queenctl apply -f`),
// through the decoder's own function, so this package needs no YAML library.
func (s *ReadSources) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var v interface{}
	if err := unmarshal(&v); err != nil {
		return err
	}
	return s.read(v)
}

func (s *ReadSources) read(v interface{}) error {
	switch x := v.(type) {
	case nil:
		*s = nil
		return nil
	case string:
		*s = ReadSources{{Queue: x}}
		return nil
	case []interface{}:
		out := make(ReadSources, 0, len(x))
		for i, item := range x {
			switch it := item.(type) {
			case string:
				out = append(out, ReadSource{Queue: it})
			case map[string]interface{}:
				q, _ := it["queue"].(string)
				if q == "" {
					return fmt.Errorf("from[%d]: a source is a queue name or {queue, where}", i)
				}
				out = append(out, ReadSource{Queue: q, Where: it["where"]})
			default:
				return fmt.Errorf("from[%d]: a source is a queue name or {queue, where}", i)
			}
		}
		*s = out
		return nil
	}
	return errors.New("from: a queue name, or a list of names and {queue, where}")
}

// ---- link ------------------------------------------------------------------------

// LinkSource names the message a link points at, the way a read returned it:
// a popped message by PartitionID, Offset and TransactionID (with the
// ConsumerGroup it was popped with); a feed entry, whose partition id is not
// the one a pop answers, by Queue, Partition, Offset and TransactionID. The
// offset finds the message and the transaction id proves it is the one meant:
// the broker answers source_mismatch otherwise.
type LinkSource struct {
	PartitionID string `json:"partitionId,omitempty"`
	Queue       string `json:"queue,omitempty"`
	Partition   string `json:"partition,omitempty"`
	Offset      int64  `json:"offset"`
	// TransactionID is the message's own: the link keeps it.
	TransactionID string `json:"transactionId"`
	// ConsumerGroup is the group the message was popped with: a scoped
	// credential must be allowed to consume the source with it. Empty: queue
	// mode.
	ConsumerGroup string `json:"consumerGroup,omitempty"`
}

// LinkOptions are TransactionQueueBuilder.Link's options.
type LinkOptions struct {
	// Headers are the link's own headers: a reader of the target sees the
	// source message's headers with these written over them, the link winning
	// on a name both have. This is how a hop adds a stamp or its trace context
	// without touching the payload everyone shares.
	Headers map[string]string
}

// linkSourceOfMessage is a popped message as a link source.
func linkSourceOfMessage(m *Message) (LinkSource, error) {
	if m == nil {
		return LinkSource{}, errors.New("link: a nil message")
	}
	if m.PartitionID == "" || m.TransactionID == "" {
		return LinkSource{}, errors.New("link: the message has no partitionId or transactionId (link a message a pop or the feed returned)")
	}
	return LinkSource{PartitionID: m.PartitionID, Offset: m.Offset, TransactionID: m.TransactionID, ConsumerGroup: m.ConsumerGroup}, nil
}

// linkSourceOfEntry is a feed entry as a link source.
func linkSourceOfEntry(e *feed.Entry) (LinkSource, error) {
	if e == nil {
		return LinkSource{}, errors.New("link: a nil feed entry")
	}
	if e.Queue == "" || e.TransactionID == "" {
		return LinkSource{}, errors.New("link: the feed entry has no queue or transactionId")
	}
	partition := e.Partition
	if partition == "" {
		partition = DefaultPartition
	}
	return LinkSource{Queue: e.Queue, Partition: partition, Offset: e.Offset, TransactionID: e.TransactionID}, nil
}

// linkSourcesOf reads what Link takes: *Message, Message, []*Message,
// []Message, FeedEntry, *FeedEntry, []FeedEntry, LinkSource or []LinkSource.
func linkSourcesOf(messages interface{}) ([]LinkSource, error) {
	var out []LinkSource
	add := func(s LinkSource, err error) error {
		if err != nil {
			return err
		}
		out = append(out, s)
		return nil
	}
	var err error
	switch m := messages.(type) {
	case *Message:
		err = add(linkSourceOfMessage(m))
	case Message:
		err = add(linkSourceOfMessage(&m))
	case []*Message:
		for _, x := range m {
			if err = add(linkSourceOfMessage(x)); err != nil {
				break
			}
		}
	case []Message:
		for i := range m {
			if err = add(linkSourceOfMessage(&m[i])); err != nil {
				break
			}
		}
	case feed.Entry:
		err = add(linkSourceOfEntry(&m))
	case *feed.Entry:
		err = add(linkSourceOfEntry(m))
	case []feed.Entry:
		for i := range m {
			if err = add(linkSourceOfEntry(&m[i])); err != nil {
				break
			}
		}
	case LinkSource:
		out = append(out, m)
	case []LinkSource:
		out = append(out, m...)
	default:
		return nil, fmt.Errorf("link: expected *Message, []*Message, []Message, a FeedEntry, []FeedEntry or LinkSource values, got %T", messages)
	}
	if err != nil {
		return nil, err
	}
	for _, s := range out {
		if s.TransactionID == "" || (s.PartitionID == "" && s.Queue == "") {
			return nil, errors.New("link: a source needs its transactionId, and a partitionId or a queue")
		}
	}
	return out, nil
}

// linkOperation is one link op as the transaction body carries it.
func linkOperation(queue, partition string, from LinkSource, headers map[string]string) Operation {
	f := from
	return Operation{Type: "link", Queue: queue, Partition: partition, From: &f, Headers: headers}
}
