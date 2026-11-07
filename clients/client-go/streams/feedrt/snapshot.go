package feedrt

import (
	"bytes"
	"compress/flate"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// Snapshots of a feed stream's state, in KV, in chunks (snapshot.js).
//
// A snapshot is the state of every stateful step at one position, as LAYERS:
// a full one, then deltas (the entries set or deleted since the layer
// before), applied in order. A layer is written in PIECES: a few thousand
// entries of one step as JSON ({ v: 2, s, k, set, del }, about 256 KB),
// deflated (raw, RFC 1951), base64, cut into KV values of 48,000 chars (a
// value with m: 1 goes on in the next).
//
//	queen-feed / <id>                 the position record: { snapshot: { layers: [{ sid, chunks, bytes, full? }], position, clock, metas, units } }
//	queen-feed / <id>#snap#<sid>#<n>  chunk n of layer sid: { d: '<base64>', m?: 1 }
//
// Format 1 (one JSON of the whole state across the chunks of one id, no
// layers) is still read: the next snapshot after it is a full layer.
const (
	SnapshotFormat    = 1
	LayerFormat       = 2
	DefaultChunkChars = 48_000
	// DefaultPieceBytes is the JSON of one piece before deflate.
	DefaultPieceBytes = 256 * 1024
	// MaxLayers is the most layers a snapshot names: a full one and deltas.
	MaxLayers = 16
	// A transaction carries at most 64 KV operations; the lock's guard is one.
	opsPerTx = 60
	// Keys asked per read: about 3 MB of chunks, under the 4 MiB read budget.
	keysPerRead = 64
)

// SnapshotError is a snapshot that cannot be read: Fatal ones stop the runner.
type SnapshotError struct {
	Msg   string
	Code  string
	Fatal bool
}

func (e *SnapshotError) Error() string { return e.Msg }

func fatalErr(code, format string, args ...interface{}) error {
	return &SnapshotError{Msg: fmt.Sprintf(format, args...), Code: code, Fatal: true}
}

// layersOf is the layers a snapshot pointer names, oldest first: format 2
// lists them; format 1 is one full layer of its own (v: 1); an empty state names none.
func layersOf(snap interface{}) []interface{} {
	m, ok := snap.(map[string]interface{})
	if !ok {
		return nil
	}
	if l, ok := m["layers"].([]interface{}); ok {
		return l
	}
	id := m["id"]
	if jsv.Truthy(id) {
		if n, ok := num(m, "chunks"); ok && n > 0 {
			b, _ := num(m, "bytes")
			return []interface{}{map[string]interface{}{"sid": id, "chunks": n, "bytes": b, "full": true, "v": 1.0}}
		}
	}
	return nil
}

func layerSid(l interface{}) string { return jsv.String(jsv.Get(l, "sid")) }

func layerChunks(l interface{}) int {
	n, _ := num(l, "chunks")
	return int(n)
}

func layerBytes(l interface{}) float64 {
	n, _ := num(l, "bytes")
	return n
}

// pointerOf is a pointer that names layers (sums kept for the metrics and
// for the old readers of id / chunks). units: how many closes the stream had
// made when the state was as the layers hold it.
func pointerOf(layers []interface{}, position, clock, metas interface{}, units *int) map[string]interface{} {
	chunks, b := 0.0, 0.0
	for _, l := range layers {
		chunks += float64(layerChunks(l))
		b += layerBytes(l)
	}
	var id interface{}
	if len(layers) > 0 {
		id = jsv.Get(layers[len(layers)-1], "sid")
	}
	if layers == nil {
		layers = []interface{}{}
	}
	p := map[string]interface{}{"id": id, "chunks": chunks, "bytes": b, "layers": layers, "position": position, "clock": clock, "metas": metas}
	if units != nil {
		p["units"] = float64(*units)
	}
	return p
}

// layerKeys is every chunk key of the layers.
func layerKeys(id string, layers []interface{}) []string {
	var out []string
	for _, l := range layers {
		sid := layerSid(l)
		for n := 0; n < layerChunks(l); n++ {
			out = append(out, chunkKey(id, sid, n))
		}
	}
	return out
}

func chunkPrefix(id string) string { return id + "#snap#" }

func chunkKey(id, sid string, n int) string { return chunkPrefix(id) + sid + "#" + strconv.Itoa(n) }

// newSnapshotID is a new snapshot id: never one an earlier attempt or another process used.
func newSnapshotID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return strconv.FormatInt(time.Now().UnixMilli(), 36) + hex.EncodeToString(b[:])
}

func deflateRaw(data []byte) []byte {
	var buf bytes.Buffer
	w, _ := flate.NewWriter(&buf, 6)
	_, _ = w.Write(data)
	_ = w.Close()
	return buf.Bytes()
}

func inflateRaw(data []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()
	return io.ReadAll(r)
}

func dumpJSON(dump []StepDump) interface{} {
	steps := make([]interface{}, len(dump))
	for i, s := range dump {
		entries := make([]interface{}, len(s.Entries))
		for j, e := range s.Entries {
			entries[j] = []interface{}{e.K, e.V}
		}
		steps[i] = jsv.Ordered{{K: "name", V: s.Name}, {K: "kind", V: s.Kind}, {K: "entries", V: entries}}
	}
	return steps
}

// EncodeSnapshot is a dump as chunks of format 1 (bytes: the JSON before deflate).
func EncodeSnapshot(dump []StepDump, chunkChars int) ([]string, int) {
	json := jsv.MustMarshal(jsv.Ordered{{K: "v", V: float64(SnapshotFormat)}, {K: "steps", V: dumpJSON(dump)}})
	b64 := base64.StdEncoding.EncodeToString(deflateRaw(json))
	size := chunkChars
	if size < 1024 {
		size = 1024
	}
	var chunks []string
	for i := 0; i < len(b64); i += size {
		end := i + size
		if end > len(b64) {
			end = len(b64)
		}
		chunks = append(chunks, b64[i:end])
	}
	return chunks, len(json)
}

// DecodeSnapshot reads chunks of format 1.
func DecodeSnapshot(chunks []string) ([]StepDump, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.Join(chunks, ""))
	if err != nil {
		return nil, err
	}
	buf, err := inflateRaw(raw)
	if err != nil {
		return nil, err
	}
	d, err := jsv.Unmarshal(buf)
	if err != nil {
		return nil, err
	}
	if v, _ := num(d, "v"); v != SnapshotFormat {
		return nil, fmt.Errorf("feed snapshot: format %s is not one this client reads (%d)", jsv.String(jsv.Get(d, "v")), SnapshotFormat)
	}
	steps, _ := jsv.Get(d, "steps").([]interface{})
	out := make([]StepDump, len(steps))
	for i, s := range steps {
		out[i] = StepDump{Name: str(s, "name"), Kind: str(s, "kind")}
		entries, _ := jsv.Get(s, "entries").([]interface{})
		for _, e := range entries {
			pair, ok := e.([]interface{})
			if !ok || len(pair) == 0 {
				continue
			}
			var v interface{} = undefinedValue
			if len(pair) > 1 {
				v = pair[1]
			}
			out[i].Entries = append(out[i].Entries, KV{K: jsv.String(pair[0]), V: v})
		}
	}
	return out, nil
}

func commitKV(ctx context.Context, b Broker, lock Lock, fill func(Tx)) error {
	tx := b.Transaction()
	if lock != nil {
		tx.Guard(lock)
	}
	fill(tx)
	res, err := tx.Commit(ctx)
	if err != nil {
		return err
	}
	if !res.Success {
		reason := res.Reason
		if reason == "" {
			reason = "unknown"
		}
		return &TxError{Reason: res.Reason, Message: fmt.Sprintf("feed snapshot: a KV write was refused (%s): %s", reason, res.Error)}
	}
	return nil
}

type layerStep struct {
	kind    string
	entries []KV
}

type layerWritten struct {
	chunks  int
	bytes   int
	entries int
}

// writeLayer writes one layer sid: for each step, its entries (a value of
// Undefined deletes the key), in pieces. Nothing points to the layer until
// the position does.
func writeLayer(ctx context.Context, b Broker, ns, id, sid string, lock Lock, steps []layerStep, chunkChars, pieceBytes int) (layerWritten, error) {
	size := chunkChars
	if size < 1024 {
		size = 1024
	}
	room := pieceBytes
	if room < 1024 {
		room = 1024
	}
	var w layerWritten
	type chunk struct {
		n     int
		value map[string]interface{}
	}
	var batch []chunk
	send := func() error {
		if len(batch) == 0 {
			return nil
		}
		part := batch
		batch = nil
		return commitKV(ctx, b, lock, func(tx Tx) {
			for _, c := range part {
				tx.KVPut(ns, chunkKey(id, sid, c.n), c.value, nil, false, true)
			}
		})
	}
	piece := func(s int, kind string, set, del []string) error {
		var buf strings.Builder
		buf.WriteString(`{"v":`)
		buf.WriteString(strconv.Itoa(LayerFormat))
		buf.WriteString(`,"s":`)
		buf.WriteString(strconv.Itoa(s))
		buf.WriteString(`,"k":`)
		buf.Write(jsv.AppendString(nil, kind))
		buf.WriteString(`,"set":[`)
		buf.WriteString(strings.Join(set, ","))
		buf.WriteString(`],"del":[`)
		buf.WriteString(strings.Join(del, ","))
		buf.WriteString(`]}`)
		json := buf.String()
		w.bytes += len(json)
		b64 := base64.StdEncoding.EncodeToString(deflateRaw([]byte(json)))
		for i := 0; i < len(b64); i += size {
			end := i + size
			more := end < len(b64)
			if end > len(b64) {
				end = len(b64)
			}
			v := map[string]interface{}{"d": b64[i:end]}
			if more {
				v["m"] = 1.0
			}
			batch = append(batch, chunk{n: w.chunks, value: v})
			w.chunks++
			if len(batch) >= opsPerTx {
				if err := send(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for s, st := range steps {
		var set, del []string
		used := 0
		for _, e := range st.entries {
			w.entries++
			// The limit is in UTF-8 bytes (JS Buffer.byteLength): len(j).
			if jsv.IsUndefined(e.V) {
				j := string(jsv.AppendString(nil, e.K))
				del = append(del, j)
				used += len(j)
			} else {
				jb, err := jsv.Marshal([]interface{}{e.K, e.V})
				if err != nil {
					return w, err
				}
				j := string(jb)
				set = append(set, j)
				used += len(j)
			}
			if used >= room {
				if err := piece(s, st.kind, set, del); err != nil {
					return w, err
				}
				set, del, used = nil, nil, 0
			}
		}
		if len(set) > 0 || len(del) > 0 {
			if err := piece(s, st.kind, set, del); err != nil {
				return w, err
			}
		}
	}
	if err := send(); err != nil {
		return w, err
	}
	return w, nil
}

// piece is one piece of a layer as read: {v, s, k, set, del}.
type piece struct {
	s   int
	k   string
	set []interface{}
	del []interface{}
}

// readLayer reads layer sid (count chunks) piece by piece, in order, and
// hands each to onPiece. Fails when a chunk is missing.
func readLayer(ctx context.Context, b Broker, ns, id, sid string, count int, onPiece func(p piece) error) error {
	var parts []string
	for i := 0; i < count; i += keysPerRead {
		to := i + keysPerRead
		if to > count {
			to = count
		}
		values, err := readValues(ctx, b, ns, id, sid, i, to, count)
		if err != nil {
			return err
		}
		for _, v := range values {
			parts = append(parts, str(v, "d"))
			if jsv.Truthy(jsv.Get(v, "m")) {
				continue
			}
			raw, err := base64.StdEncoding.DecodeString(strings.Join(parts, ""))
			if err != nil {
				return err
			}
			parts = nil
			buf, err := inflateRaw(raw)
			if err != nil {
				return err
			}
			p, err := jsv.Unmarshal(buf)
			if err != nil {
				return err
			}
			if f, _ := num(p, "v"); f != LayerFormat {
				return fatalErr("feed_snapshot_format", "feed snapshot %s of %s: a piece of format %s (this client reads %d)", sid, id, jsv.String(jsv.Get(p, "v")), LayerFormat)
			}
			s, _ := num(p, "s")
			set, _ := jsv.Get(p, "set").([]interface{})
			del, _ := jsv.Get(p, "del").([]interface{})
			if err := onPiece(piece{s: int(s), k: str(p, "k"), set: set, del: del}); err != nil {
				return err
			}
		}
	}
	if len(parts) > 0 {
		return fatalErr("feed_snapshot_missing", "feed snapshot %s of %s: its last chunk says more follows", sid, id)
	}
	return nil
}

// readValues answers chunks from to to (exclusive) of layer sid, as their values, in order.
func readValues(ctx context.Context, b Broker, ns, id, sid string, from, to, count int) ([]interface{}, error) {
	out := make([]interface{}, to-from)
	var ask []string
	for n := from; n < to; n++ {
		ask = append(ask, chunkKey(id, sid, n))
	}
	for round := 0; len(ask) > 0; round++ {
		r, err := b.KVGetMany(ctx, ns, ask)
		if err != nil {
			return nil, err
		}
		got := map[string]bool{}
		for _, row := range r.Rows {
			n, _ := strconv.Atoi(row.Key[strings.LastIndex(row.Key, "#")+1:])
			if n-from >= 0 && n-from < len(out) {
				if _, ok := jsv.Get(row.Value, "d").(string); ok {
					out[n-from] = row.Value
				} else {
					out[n-from] = nil
				}
			}
			got[row.Key] = true
		}
		if len(r.Missing) > 0 {
			return nil, fatalErr("feed_snapshot_missing", "feed snapshot %s of %s lacks %d of its %d chunks (first: %s): "+
				"the state cannot be loaded; start the stream over with { reset: true }", sid, id, len(r.Missing), count, r.Missing[0])
		}
		var left []string
		for _, k := range ask {
			if !got[k] {
				left = append(left, k)
			}
		}
		if len(left) == len(ask) || round > 64 {
			return nil, fmt.Errorf("feed snapshot: the KV read of %s's chunks came back empty and cut", id)
		}
		ask = left
	}
	for n, v := range out {
		if v == nil {
			return nil, fatalErr("feed_snapshot_missing", "feed snapshot %s of %s: chunk %d is not a chunk", sid, id, from+n)
		}
	}
	return out, nil
}

// writeChunks writes the chunks of snapshot sid (format 1); nothing points to them until the position does.
func writeChunks(ctx context.Context, b Broker, ns, id, sid string, chunks []string, lock Lock) error {
	for i := 0; i < len(chunks); i += opsPerTx {
		end := i + opsPerTx
		if end > len(chunks) {
			end = len(chunks)
		}
		lo, hi := i, end
		if err := commitKV(ctx, b, lock, func(tx Tx) {
			for n := lo; n < hi; n++ {
				tx.KVPut(ns, chunkKey(id, sid, n), map[string]interface{}{"d": chunks[n]}, nil, false, true)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

// readChunks answers the chunks of snapshot sid (format 1), in order.
func readChunks(ctx context.Context, b Broker, ns, id, sid string, count int) ([]string, error) {
	out := make([]string, count)
	have := make([]bool, count)
	for i := 0; i < count; i += keysPerRead {
		var ask []string
		for n := i; n < count && n < i+keysPerRead; n++ {
			ask = append(ask, chunkKey(id, sid, n))
		}
		for round := 0; len(ask) > 0; round++ {
			r, err := b.KVGetMany(ctx, ns, ask)
			if err != nil {
				return nil, err
			}
			got := map[string]bool{}
			for _, row := range r.Rows {
				n, _ := strconv.Atoi(row.Key[strings.LastIndex(row.Key, "#")+1:])
				if n >= 0 && n < count {
					if d, ok := jsv.Get(row.Value, "d").(string); ok {
						out[n], have[n] = d, true
					} else {
						have[n] = false
					}
				}
				got[row.Key] = true
			}
			if len(r.Missing) > 0 {
				return nil, fatalErr("feed_snapshot_missing", "feed snapshot %s of %s lacks %d of its %d chunks (first: %s): "+
					"the state cannot be loaded; start the stream over with { reset: true }", sid, id, len(r.Missing), count, r.Missing[0])
			}
			var left []string
			for _, k := range ask {
				if !got[k] {
					left = append(left, k)
				}
			}
			if len(left) == len(ask) || round > 64 {
				return nil, fmt.Errorf("feed snapshot: the KV read of %s's chunks came back empty and cut", id)
			}
			ask = left
		}
	}
	for n := 0; n < count; n++ {
		if !have[n] {
			return nil, fatalErr("feed_snapshot_missing", "feed snapshot %s of %s: chunk %d is not a chunk", sid, id, n)
		}
	}
	return out, nil
}

// deleteKeys deletes keys, in transactions of at most 60 deletes.
func deleteKeys(ctx context.Context, b Broker, ns string, keys []string, lock Lock) error {
	for i := 0; i < len(keys); i += opsPerTx {
		end := i + opsPerTx
		if end > len(keys) {
			end = len(keys)
		}
		part := keys[i:end]
		if err := commitKV(ctx, b, lock, func(tx Tx) {
			for _, k := range part {
				tx.KVDelete(ns, k)
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

// strayChunkKeys is the chunk keys of id in KV whose layer is not one of keep
// (left by an attempt that never committed).
func strayChunkKeys(ctx context.Context, b Broker, ns, id string, keep map[string]bool) ([]string, error) {
	prefix := chunkPrefix(id)
	var out []string
	after := ""
	for page := 0; page < 1000; page++ {
		r, err := b.KVGetPrefix(ctx, ns, prefix, true, 1000, after)
		if err != nil {
			return nil, err
		}
		for _, row := range r.Rows {
			key := row.Key
			last := strings.LastIndex(key, "#")
			if last < len(prefix) {
				continue
			}
			sid := key[len(prefix):last]
			if !keep[sid] {
				out = append(out, key)
			}
		}
		if !r.Truncated || r.NextAfter == "" {
			break
		}
		after = r.NextAfter
	}
	return out, nil
}
