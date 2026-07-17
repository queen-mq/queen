package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	cfg      Config
	pool     *pgxpool.Pool
	engine   *Engines
	metrics  *Metrics
	notifier *Notifier
	auth     *Auth
	crypto   *Crypto
}

// ---- push ------------------------------------------------------------------

type pushBody struct {
	Items []struct {
		Queue         string          `json:"queue"`
		Partition     string          `json:"partition"`
		Payload       json.RawMessage `json:"payload"`
		TransactionID string          `json:"transactionId"`
		TraceID       string          `json:"traceId"`
	} `json:"items"`
}

func (s *Server) handlePush(w http.ResponseWriter, r *http.Request) {
	if s.auth != nil && !s.auth.check() {
		httpError(w, 401, "unauthorized")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		httpError(w, 400, "read body: "+err.Error())
		return
	}
	var pb pushBody
	if err := json.Unmarshal(body, &pb); err != nil {
		httpError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	if len(pb.Items) == 0 {
		writeRawArray(w, 201, nil)
		return
	}

	items := make([][]byte, len(pb.Items))
	partSet := make(map[string]struct{})
	for i := range pb.Items {
		it := &pb.Items[i]
		partition := it.Partition
		if partition == "" {
			partition = "Default"
		}
		partSet[it.Queue+"\x1f"+partition] = struct{}{}
		var buf bytes.Buffer
		buf.WriteString(`{"queue":`)
		appendJSONString(&buf, it.Queue)
		buf.WriteString(`,"partition":`)
		appendJSONString(&buf, partition)
		buf.WriteString(`,"payload":`)
		encrypted := false
		if s.crypto != nil {
			pt := []byte(it.Payload)
			if len(pt) == 0 {
				pt = []byte("{}")
			}
			buf.Write(s.crypto.encryptPayload(pt))
			encrypted = true
		} else if len(it.Payload) > 0 {
			buf.Write(it.Payload) // RAW pass-through (no re-serialization)
		} else {
			buf.WriteString(`{}`)
		}
		if encrypted {
			buf.WriteString(`,"is_encrypted":true,"messageId":`)
		} else {
			buf.WriteString(`,"is_encrypted":false,"messageId":`)
		}
		appendJSONString(&buf, generateUUIDv7())
		if it.TransactionID != "" {
			buf.WriteString(`,"transactionId":`)
			appendJSONString(&buf, it.TransactionID)
		}
		if it.TraceID != "" {
			buf.WriteString(`,"traceId":`)
			appendJSONString(&buf, it.TraceID)
		}
		buf.WriteByte('}')
		items[i] = buf.Bytes()
	}

	parts := make([]string, 0, len(partSet))
	for p := range partSet {
		parts = append(parts, p)
	}
	elems, err := s.engine.submit(lanePush, items, parts)
	if err != nil {
		httpError(w, 500, err.Error())
		return
	}
	s.metrics.Push.recordRequest(len(items))
	// Wake any long-poll pops parked on the queues we just committed to
	// (analogue of libqueen's update_pop_backoff_tracker). Notify by queue.
	for i := range pb.Items {
		s.notifier.Notify(pb.Items[i].Queue)
	}
	writeRawArray(w, 201, elems)
}

// ---- pop -------------------------------------------------------------------

func (s *Server) handlePop(w http.ResponseWriter, r *http.Request) {
	if s.auth != nil && !s.auth.check() {
		httpError(w, 401, "unauthorized")
		return
	}
	queue := r.PathValue("queue")
	partition := r.PathValue("partition") // "" for wildcard route
	q := r.URL.Query()

	consumerGroup := q.Get("consumerGroup")
	if consumerGroup == "" {
		consumerGroup = "__QUEUE_MODE__"
	}
	batch := atoiDefault(q.Get("batch"), 1)
	maxPartitions := atoiDefault(q.Get("partitions"), 1)
	if maxPartitions < 1 {
		maxPartitions = 1
	}
	autoAck := q.Get("autoAck") == "true"
	subMode := q.Get("subscriptionMode")
	subFrom := q.Get("subscriptionFrom")
	wait := q.Get("wait") == "true"
	timeoutMs := atoiDefault(q.Get("timeout"), s.cfg.PopDefaultTimeoutMs)

	// Fresh worker_id (leaseId) per SP attempt, matching the C++ pop route.
	buildReq := func() []byte {
		var buf bytes.Buffer
		buf.WriteString(`{"queue_name":`)
		appendJSONString(&buf, queue)
		buf.WriteString(`,"partition_name":`)
		appendJSONString(&buf, partition)
		buf.WriteString(`,"consumer_group":`)
		appendJSONString(&buf, consumerGroup)
		buf.WriteString(`,"batch_size":`)
		buf.WriteString(strconv.Itoa(batch))
		buf.WriteString(`,"lease_seconds":0,"worker_id":`)
		appendJSONString(&buf, generateUUIDv7())
		buf.WriteString(`,"sub_mode":`)
		appendJSONString(&buf, subMode)
		buf.WriteString(`,"sub_from":`)
		appendJSONString(&buf, subFrom)
		buf.WriteString(`,"auto_ack":`)
		buf.WriteString(strconv.FormatBool(autoAck))
		buf.WriteString(`,"max_partitions":`)
		buf.WriteString(strconv.Itoa(maxPartitions))
		buf.WriteByte('}')
		return buf.Bytes()
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	interval := s.cfg.PopWaitInitialMs
	empties := 0
	ctx := r.Context()

	for {
		// Register interest BEFORE the attempt so a concurrent push can't slip a
		// wakeup between the empty result and the park (lost-wakeup safe).
		waitCh := s.notifier.Register(queue)

		elems, err := s.engine.submit(lanePop, [][]byte{buildReq()}, nil)
		if err != nil {
			httpError(w, 500, err.Error())
			return
		}
		var result json.RawMessage
		if len(elems) > 0 {
			result = extractResultObject(elems[0]) // raw slice, no JSON parse
		}
		// Count messages cheaply (one transactionId per message) — avoids
		// parsing the whole result object on the hot path.
		n := 0
		if len(result) > 0 {
			n = bytes.Count(result, txnIDKey)
		}

		if n > 0 || !wait {
			s.metrics.Pop.recordRequest(n)
			if n == 0 {
				writeJSON(w, 204, []byte(`{"messages":[]}`))
			} else {
				if s.crypto != nil {
					result = s.decryptPopResult(result)
				}
				writeJSON(w, 200, result)
			}
			return
		}

		// wait=true and empty: PARK until a push wakes this queue, the backoff
		// timer elapses, the deadline passes, or the client disconnects — instead
		// of blindly re-polling (libqueen backoff-tracker + wake-on-push).
		remaining := time.Until(deadline)
		if remaining <= 0 {
			s.metrics.Pop.recordRequest(0)
			writeJSON(w, 204, []byte(`{"messages":[]}`))
			return
		}
		sleep := time.Duration(interval) * time.Millisecond
		if sleep > remaining {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-waitCh:
			// Woken by a push to this queue: reset the backoff (data is likely).
			timer.Stop()
			interval = s.cfg.PopWaitInitialMs
			empties = 0
		case <-timer.C:
			empties++
			if empties >= s.cfg.PopWaitThreshold {
				interval *= s.cfg.PopWaitMultipl
				if interval > s.cfg.PopWaitMaxMs {
					interval = s.cfg.PopWaitMaxMs
				}
			}
		}
	}
}

// ---- ack -------------------------------------------------------------------

type ackBatchBody struct {
	Acknowledgments []map[string]json.RawMessage `json:"acknowledgments"`
	ConsumerGroup   string                       `json:"consumerGroup"`
}

func (s *Server) handleAckBatch(w http.ResponseWriter, r *http.Request) {
	if s.auth != nil && !s.auth.check() {
		httpError(w, 401, "unauthorized")
		return
	}
	body, _ := io.ReadAll(r.Body)
	var ab ackBatchBody
	if err := json.Unmarshal(body, &ab); err != nil {
		httpError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	if len(ab.Acknowledgments) == 0 {
		httpError(w, 400, "acknowledgments array is required")
		return
	}
	cg := ab.ConsumerGroup
	if cg == "" {
		cg = "__QUEUE_MODE__"
	}
	items := make([][]byte, len(ab.Acknowledgments))
	for i, a := range ab.Acknowledgments {
		items[i] = buildAckObject(a, cg)
	}
	elems, err := s.engine.submit(laneAck, items, nil)
	if err != nil {
		httpError(w, 500, err.Error())
		return
	}
	s.metrics.Ack.recordRequest(len(items))
	writeRawArray(w, 200, elems)
}

func (s *Server) handleAckSingle(w http.ResponseWriter, r *http.Request) {
	if s.auth != nil && !s.auth.check() {
		httpError(w, 401, "unauthorized")
		return
	}
	body, _ := io.ReadAll(r.Body)
	var a map[string]json.RawMessage
	if err := json.Unmarshal(body, &a); err != nil {
		httpError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	cg := "__QUEUE_MODE__"
	if v, ok := a["consumerGroup"]; ok {
		var s string
		if json.Unmarshal(v, &s) == nil && s != "" {
			cg = s
		}
	}
	elems, err := s.engine.submit(laneAck, [][]byte{buildAckObject(a, cg)}, nil)
	if err != nil {
		httpError(w, 500, err.Error())
		return
	}
	s.metrics.Ack.recordRequest(1)
	// Single ack: clients read the object; return the first element.
	if len(elems) > 0 {
		writeJSON(w, 200, elems[0])
		return
	}
	writeJSON(w, 200, []byte(`{"success":false}`))
}

func buildAckObject(a map[string]json.RawMessage, cg string) []byte {
	var buf bytes.Buffer
	buf.WriteByte('{')
	first := true
	put := func(k string, v []byte) {
		if !first {
			buf.WriteByte(',')
		}
		first = false
		appendJSONString(&buf, k)
		buf.WriteByte(':')
		buf.Write(v)
	}
	// pass through known ack fields verbatim (already valid JSON values)
	for _, k := range []string{"transactionId", "partitionId", "leaseId", "status", "error"} {
		if v, ok := a[k]; ok && len(v) > 0 {
			put(k, v)
		}
	}
	// consumerGroup is server-authoritative
	cgBytes, _ := json.Marshal(cg)
	put("consumerGroup", cgBytes)
	buf.WriteByte('}')
	return buf.Bytes()
}

// ---- configure / status ----------------------------------------------------

func (s *Server) handleConfigure(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		httpError(w, 400, "invalid JSON: "+err.Error())
		return
	}
	qraw, ok := m["queue"]
	if !ok {
		httpError(w, 400, "queue is required")
		return
	}
	var queue string
	_ = json.Unmarshal(qraw, &queue)

	// forward options (+namespace/task) to configure_queue_v1, mirroring the C++ route.
	opts := map[string]json.RawMessage{}
	if o, ok := m["options"]; ok {
		_ = json.Unmarshal(o, &opts)
	}
	if v, ok := m["namespace"]; ok {
		opts["namespace"] = v
	}
	if v, ok := m["task"]; ok {
		opts["task"] = v
	}
	optsJSON, _ := json.Marshal(opts)

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.StmtTimeout)
	defer cancel()
	var raw []byte
	err := s.pool.QueryRow(ctx, "SELECT queen.configure_queue_v1($1, $2::jsonb)", queue, string(optsJSON)).Scan(&raw)
	if err != nil {
		httpError(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, raw)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, []byte(`{"status":"ok","engine":"go-hotpath-spike"}`))
}

// ---- helpers ---------------------------------------------------------------

func appendJSONString(buf *bytes.Buffer, s string) {
	b, _ := json.Marshal(s)
	buf.Write(b)
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func writeRawArray(w http.ResponseWriter, status int, elems []json.RawMessage) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if len(elems) == 0 {
		_, _ = w.Write([]byte("[]"))
		return
	}
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, e := range elems {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(e)
	}
	buf.WriteByte(']')
	_, _ = w.Write(buf.Bytes())
}

func httpError(w http.ResponseWriter, status int, msg string) {
	b, _ := json.Marshal(map[string]string{"error": msg})
	writeJSON(w, status, b)
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

var txnIDKey = []byte(`"transactionId"`)

// extractResultObject returns the raw bytes of the "result" object inside a
// pop element {"idx":N,"result":{...}} WITHOUT parsing it (brace-matched,
// string-aware). Mirrors the C++ simdjson raw-slice delivery.
func extractResultObject(raw []byte) []byte {
	key := []byte(`"result":`)
	i := bytes.Index(raw, key)
	if i < 0 {
		return nil
	}
	j := i + len(key)
	for j < len(raw) && (raw[j] == ' ' || raw[j] == '\t' || raw[j] == '\n' || raw[j] == '\r') {
		j++
	}
	if j >= len(raw) || raw[j] != '{' {
		return nil
	}
	depth, inStr, esc, start := 0, false, false, j
	for ; j < len(raw); j++ {
		c := raw[j]
		if inStr {
			switch {
			case esc:
				esc = false
			case c == '\\':
				esc = true
			case c == '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return raw[start : j+1]
			}
		}
	}
	return nil
}

// decryptPopResult parses a pop result object and decrypts each message's data
// field (the C++ pop_decrypt_messages equivalent). Full parse + per-message
// AES-GCM open — a real cost when encryption is enabled.
func (s *Server) decryptPopResult(result []byte) []byte {
	var obj map[string]json.RawMessage
	if json.Unmarshal(result, &obj) != nil {
		return result
	}
	msgsRaw, ok := obj["messages"]
	if !ok {
		return result
	}
	var msgs []map[string]json.RawMessage
	if json.Unmarshal(msgsRaw, &msgs) != nil {
		return result
	}
	changed := false
	for _, m := range msgs {
		dataRaw, ok := m["data"]
		if !ok {
			continue
		}
		var enc struct {
			Encrypted string `json:"encrypted"`
			Iv        string `json:"iv"`
			AuthTag   string `json:"authTag"`
		}
		if json.Unmarshal(dataRaw, &enc) == nil && enc.Encrypted != "" {
			if pt, ok := s.crypto.decrypt(enc.Encrypted, enc.Iv, enc.AuthTag); ok {
				m["data"] = json.RawMessage(pt)
				changed = true
			}
		}
	}
	if !changed {
		return result
	}
	nm, _ := json.Marshal(msgs)
	obj["messages"] = nm
	out, _ := json.Marshal(obj)
	return out
}

func (s *Server) handlePrometheus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(s.metrics.Prometheus()))
}
