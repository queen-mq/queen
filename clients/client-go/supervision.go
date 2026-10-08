package queen

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// SupervisionConfig opts a consume invocation into dashboard observations.
// Nil (the default) disables reporting, including its goroutine and KV traffic.
// Group names the application/deployment, independently of the consumer group.
type SupervisionConfig struct{ Group string }

var supervisionGroup = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$`)

type consumerSupervision struct {
	mu                                   sync.Mutex
	http                                 *HttpClient
	opts                                 ConsumeOptions
	group, id, host                      string
	started                              time.Time
	running, completed, failed, sequence int
	last                                 interface{}
	active                               map[int]time.Time
	stop                                 chan struct{}
	done                                 chan struct{}
}

func newConsumerSupervision(http *HttpClient, opts ConsumeOptions) (*consumerSupervision, error) {
	if opts.Supervision == nil {
		return nil, nil
	}
	if !supervisionGroup.MatchString(opts.Supervision.Group) || opts.Supervision.Group == "coordination" {
		return nil, fmt.Errorf("supervision group must be a valid application/deployment name")
	}
	if opts.Concurrency < 1 || opts.Concurrency > 4096 {
		return nil, fmt.Errorf("supervision requires concurrency between 1 and 4096")
	}
	host, _ := os.Hostname()
	return &consumerSupervision{http: http, opts: opts, group: opts.Supervision.Group,
		id: strings.ReplaceAll(uuid.NewString(), "-", ""), host: host, started: time.Now(),
		running: opts.Concurrency, active: make(map[int]time.Time), stop: make(chan struct{}), done: make(chan struct{})}, nil
}

func (s *consumerSupervision) invoke(ctx context.Context, handler BatchMessageHandler, messages []*Message) (err error) {
	s.mu.Lock()
	id := s.sequence
	s.sequence++
	s.active[id] = time.Now()
	s.mu.Unlock()
	returned := false
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.active, id)
		s.last = time.Now().Unix()
		if returned && err == nil {
			s.completed++
		} else {
			s.failed++
		}
	}()
	err = handler(ctx, messages)
	returned = true
	return
}

func (s *consumerSupervision) workerExited() { s.mu.Lock(); s.running--; s.mu.Unlock() }
func (s *consumerSupervision) finish()       { close(s.stop); <-s.done }

func optionalScope(v string) interface{} {
	if v == "" {
		return nil
	}
	return v
}
func (s *consumerSupervision) document(state string) map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldest interface{}
	for _, started := range s.active {
		age := int(time.Since(started).Seconds())
		if oldest == nil || age > oldest.(int) {
			oldest = age
		}
	}
	group := s.opts.Group
	if group == "" {
		group = "__QUEUE_MODE__"
	}
	return map[string]interface{}{
		"schema": "queen.consumer.status/v1", "instance_id": s.id, "engine": "go", "execution_model": "goroutines",
		"hostname": s.host, "pid": os.Getpid(), "state": state, "updated_at_epoch": time.Now().Unix(),
		"started_at_epoch": s.started.Unix(), "uptime_seconds": int(time.Since(s.started).Seconds()),
		"configuration": map[string]interface{}{"heartbeat_timeout": 30},
		"pool_status": []interface{}{map[string]interface{}{"name": "consumer", "queue": optionalScope(s.opts.Queue),
			"namespace": optionalScope(s.opts.Namespace), "task": optionalScope(s.opts.Task), "consumer_group": group,
			"desired": s.opts.Concurrency, "running": s.running, "busy": len(s.active), "completed": s.completed,
			"failed": s.failed, "last_completed_at_epoch": s.last, "oldest_inflight_seconds": oldest}},
	}
}
func (s *consumerSupervision) publish(state string) error {
	data, err := json.Marshal(s.document(state))
	if err != nil {
		return err
	}
	if len(data) > 45000 {
		return fmt.Errorf("consumer status exceeds one chunk")
	}
	write := strings.ReplaceAll(uuid.NewString(), "-", "")
	slot := s.group + "/" + s.id
	ops := []interface{}{
		map[string]interface{}{"op": "put", "ns": "queen-supervisor", "key": slot + "/head", "ttlSeconds": 60,
			"value": map[string]interface{}{"format": "queen.supervisor.remote-status/v1", "write": write, "chunks": 1, "bytes": len(data)}},
		map[string]interface{}{"op": "put", "ns": "queen-supervisor", "key": slot + "/chunk/0000", "ttlSeconds": 60,
			"value": map[string]interface{}{"write": write, "index": 0, "data": base64.StdEncoding.EncodeToString(data)}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response, err := s.http.Post(ctx, "/api/v1/kv", map[string]interface{}{"operations": ops}, WithoutFailoverRetry())
	if err != nil {
		return err
	}
	results, ok := response["results"].([]interface{})
	if !ok || len(results) != 2 {
		return fmt.Errorf("consumer status publication was not applied")
	}
	for _, result := range results {
		row, ok := result.(map[string]interface{})
		if !ok || row["applied"] != true {
			return fmt.Errorf("consumer status publication was not applied")
		}
	}
	return nil
}
func (s *consumerSupervision) run() {
	defer close(s.done)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	failing := false
	send := func(state string) {
		if err := s.publish(state); err != nil {
			if !failing {
				logWarn("Consumer.supervision", map[string]interface{}{"message": "Status publication failed; consumption continues"})
			}
			failing = true
		} else {
			failing = false
		}
	}
	send("running")
	for {
		select {
		case <-s.stop:
			send("stopped")
			return
		case <-ticker.C:
			send("running")
		}
	}
}
