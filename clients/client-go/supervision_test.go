package queen

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSupervisionWireAndDefaultOff(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "off", true: "on"}[enabled], func(t *testing.T) {
			var mu sync.Mutex
			var docs []map[string]interface{}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path != "/api/v1/kv" {
					_, _ = w.Write([]byte(`{"messages":[{"transactionId":"t","partitionId":"p","data":{"private":"payload"}}]}`))
					return
				}
				var request struct {
					Operations []struct {
						NS, Key string
						TTL     int `json:"ttlSeconds"`
						Value   map[string]interface{}
					}
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				if len(request.Operations) != 2 {
					t.Error("expected one atomic head/chunk batch")
					return
				}
				head, chunk := request.Operations[0], request.Operations[1]
				if head.NS != "queen-supervisor" || head.TTL != 60 || chunk.TTL != 60 || head.Value["write"] != chunk.Value["write"] {
					t.Error("incorrect publication envelope")
				}
				raw, err := base64.StdEncoding.DecodeString(chunk.Value["data"].(string))
				if err != nil {
					t.Error(err)
					return
				}
				if len(raw) != int(head.Value["bytes"].(float64)) {
					t.Error("incorrect UTF-8 byte length")
				}
				if strings.Contains(string(raw), "private") {
					t.Error("payload leaked into status")
				}
				var doc map[string]interface{}
				_ = json.Unmarshal(raw, &doc)
				mu.Lock()
				docs = append(docs, doc)
				mu.Unlock()
				_, _ = w.Write([]byte(`{"results":[{"applied":true},{"applied":true}]}`))
			}))
			defer srv.Close()
			client := newWireClient(t, srv.URL)
			qb := client.Queue("orders").Concurrency(2).Wait(false).AutoAck(false).Each().Limit(1)
			if enabled {
				qb.Supervision(&SupervisionConfig{Group: "billing-production"})
			}
			if err := qb.Consume(context.Background(), func(context.Context, *Message) error { return nil }).Execute(context.Background()); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !enabled {
				if len(docs) != 0 {
					t.Fatal("default-off consumer published status")
				}
				return
			}
			if len(docs) < 2 {
				t.Fatal("missing lifecycle publication")
			}
			last := docs[len(docs)-1]
			pool := last["pool_status"].([]interface{})[0].(map[string]interface{})
			if last["state"] != "stopped" || pool["running"] != float64(0) || pool["busy"] != float64(0) || pool["completed"] != float64(2) {
				t.Fatalf("incorrect stopped snapshot: %v", last)
			}
		})
	}
}

func TestSupervisionFailurePreservesHandlerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/kv" {
			w.WriteHeader(403)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"messages":[{"transactionId":"t","partitionId":"p","data":{}}]}`))
	}))
	defer srv.Close()
	client := newWireClient(t, srv.URL)
	poison := errors.New("handler failure")
	err := client.Queue("orders").Supervision(&SupervisionConfig{Group: "billing"}).AutoAck(false).Wait(false).
		Consume(context.Background(), func(context.Context, *Message) error { return poison }).Execute(context.Background())
	if !errors.Is(err, poison) {
		t.Fatalf("publication changed handler outcome: %v", err)
	}
}

func TestSupervisionProgressAndIdentity(t *testing.T) {
	opts := ConsumeOptions{Queue: "orders", Concurrency: 2, Supervision: &SupervisionConfig{Group: "billing"}}
	s, err := newConsumerSupervision(nil, opts)
	if err != nil {
		t.Fatal(err)
	}
	other, _ := newConsumerSupervision(nil, opts)
	if s.id == other.id {
		t.Fatal("instance reused")
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_ = s.invoke(context.Background(), func(context.Context, []*Message) error { close(entered); <-release; return errors.New("private") }, nil)
	}()
	<-entered
	pool := s.document("running")["pool_status"].([]interface{})[0].(map[string]interface{})
	if pool["busy"] != 1 || pool["completed"] != 0 || pool["oldest_inflight_seconds"] == nil {
		t.Fatalf("incorrect in-flight observation: %v", pool)
	}
	close(release)
	<-done
	s.workerExited()
	pool = s.document("running")["pool_status"].([]interface{})[0].(map[string]interface{})
	if pool["running"] != 1 || pool["failed"] != 1 || pool["busy"] != 0 || pool["oldest_inflight_seconds"] != nil || pool["last_completed_at_epoch"] == nil {
		t.Fatalf("incorrect exit/progress observation: %v", pool)
	}
	for _, group := range []string{"", "coordination", "a/b", "a\n"} {
		opts.Supervision = &SupervisionConfig{Group: group}
		if _, err := newConsumerSupervision(nil, opts); err == nil {
			t.Fatalf("accepted invalid group %q", group)
		}
	}
}

func TestSupervisionDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	defer srv.Close()
	client := newWireClient(t, srv.URL)
	s, _ := newConsumerSupervision(client.httpClient, ConsumeOptions{Concurrency: 1, Supervision: &SupervisionConfig{Group: "billing"}})
	start := time.Now()
	if err := s.publish("running"); err == nil {
		t.Fatal("expected publication deadline")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("publication exceeded its total deadline")
	}
}
