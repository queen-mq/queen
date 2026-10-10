package broker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// The bodies below are what a 2.0.3 broker answered, shortened.
const (
	healthFollower = `{"status":"healthy","engine":"raft","version":"2.0.3","raft":{"role":"follower","leader":true,"term":1,"applied":23,"commit":23,"lag":0,"quorumAckMs":186,"storageReady":true,"clusterVersion":4,"kinds":4,"apply":{"failure":null,"skipped":[]}}}`
	healthNoQuorum = `{"status":"settling","engine":"raft","version":"2.0.3","raft":{"role":"leader","leader":true,"term":1,"applied":23,"commit":23,"lag":0,"quorumAckMs":8417,"storageReady":true,"apply":{"failure":null,"skipped":[]}}}`
	healthOld      = `{"status":"healthy","engine":"raft","version":"2.0.2","raft":{"role":"leader","leader":true,"term":4,"applied":111,"commit":111,"lag":0,"storageReady":true}}`
	healthPoisoned = `{"status":"settling","engine":"raft","version":"2.0.3","raft":{"role":"stopped","leader":false,"term":2,"applied":1158,"commit":1159,"lag":0,"storageReady":false,"apply":{"failure":{"index":1159,"class":"deterministic"},"skipped":[]}}}`
	membership     = `{"engine":"raft","membership":{"nodeId":3,"source":"leader","viewAgeMs":0,"leader":3,"term":1,"voters":[1,2,3],"learners":[4],"joint":null,"changeInFlight":false,"lastLogIndex":1055,"committedIndex":1055,"liveWithinMs":2000,"promoteMaxLag":1000,"members":[{"nodeId":1,"voter":true,"raft":"queen-0.queen-headless.queen.svc.cluster.local:7400","http":"queen-0.queen-headless.queen.svc.cluster.local:6632","matched":721,"lag":334,"lastAckMs":2,"live":true},{"nodeId":2,"voter":true,"raft":"r","http":"h","matched":1055,"lag":0,"lastAckMs":28,"live":true},{"nodeId":3,"voter":true,"raft":"r","http":"h","matched":1055,"lag":0,"lastAckMs":0,"live":true},{"nodeId":4,"voter":false,"raft":"r","http":"h","matched":null,"lag":null,"lastAckMs":null,"live":false}]}}`
)

// serve answers each "METHOD path" with a status and a body, as one pod.
func serve(t *testing.T, answers map[string][2]string) (*Client, *[]string) {
	t.Helper()
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		seen = append(seen, r.Method+" "+r.URL.Path+" "+string(body))
		a, ok := answers[r.Method+" "+r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		code, _ := strconv.Atoi(a[0])
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = io.WriteString(w, a[1])
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	host, port, _ := strings.Cut(u.Host, ":")
	p, _ := strconv.Atoi(port)
	// The pod "name" is the test server's address: Domain turns into nothing.
	return &Client{T: &hostTransport{Direct{HTTP: srv.Client(), Port: int32(p)}, host}}, &seen
}

// hostTransport sends every pod's requests to one host.
type hostTransport struct {
	d    Direct
	host string
}

func (h *hostTransport) Do(ctx context.Context, _ string, method, path string, body []byte) (int, []byte, error) {
	d := h.d
	d.Domain = "invalid"
	req, err := http.NewRequestWithContext(ctx, method, "http://"+h.host+":"+strconv.Itoa(int(d.Port))+path, strings.NewReader(string(body)))
	if err != nil {
		return 0, nil, err
	}
	res, err := d.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	return res.StatusCode, out, err
}

func TestHealthIsReadWhateverTheStatus(t *testing.T) {
	for _, c := range []struct {
		code, body string
		healthy    bool
		role       string
		quorum     *int64
		failed     bool
	}{
		{"200", healthFollower, true, "follower", ptr(186), false},
		// A node without its quorum answers 503: the body is still its health.
		{"503", healthNoQuorum, false, "leader", ptr(8417), false},
		// A broker older than the quorum figure reports none.
		{"200", healthOld, true, "leader", nil, false},
		{"503", healthPoisoned, false, "stopped", nil, true},
	} {
		cl, _ := serve(t, map[string][2]string{"GET /health": {c.code, c.body}})
		h, err := cl.Health(context.Background(), "queen-0")
		if err != nil {
			t.Fatal(err)
		}
		if h.Healthy != c.healthy || h.Raft.Role != c.role || h.ApplyFailed() != c.failed {
			t.Fatalf("%s: %+v", c.body, h)
		}
		if (h.Raft.QuorumAckMs == nil) != (c.quorum == nil) || (c.quorum != nil && *h.Raft.QuorumAckMs != *c.quorum) {
			t.Fatalf("quorumAckMs of %s: %v", c.body, h.Raft.QuorumAckMs)
		}
	}
}

func ptr(v int64) *int64 { return &v }

func TestTheMembershipIsTheLeadersView(t *testing.T) {
	cl, _ := serve(t, map[string][2]string{"GET /api/v1/system/raft/membership": {"200", membership}})
	m, err := cl.Membership(context.Background(), "queen-0")
	if err != nil {
		t.Fatal(err)
	}
	if m.Source != "leader" || *m.Leader != 3 || !m.IsVoter(1) || !m.IsLearner(4) || m.IsMember(5) || m.PromoteMaxLag != 1000 {
		t.Fatalf("%+v", m)
	}
	// Node 1 is the voter that came back empty: live, behind, never moving.
	if n := m.Member(1); n == nil || !n.Live || *n.Lag != 334 || *n.Matched != 721 {
		t.Fatalf("%+v", n)
	}
	// A learner nothing was replicated to yet has no position.
	if n := m.Member(4); n == nil || n.Lag != nil || n.Live {
		t.Fatalf("%+v", n)
	}
}

func TestAChangeTheLeaderRefusesCarriesItsCode(t *testing.T) {
	ok := `{"ok":true,"membership":{"nodeId":1,"source":"leader","leader":1,"voters":[1,2,3],"learners":[4],"members":[]}}`
	cl, seen := serve(t, map[string][2]string{
		"POST /api/v1/system/raft/membership/learners":    {"200", ok},
		"POST /api/v1/system/raft/membership/promote":     {"409", `{"ok":false,"code":"learner_behind","error":"learner 4 is 40000 entries behind"}`},
		"DELETE /api/v1/system/raft/membership/members/5": {"409", `{"ok":false,"code":"no_quorum","error":"too few live voters"}`},
	})
	ctx := context.Background()

	m, err := cl.AddLearner(ctx, "queen-0", 4, "queen-3.h:7400", "queen-3.h:6632")
	if err != nil || !m.IsLearner(4) {
		t.Fatalf("%v %+v", err, m)
	}
	var sent map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN((*seen)[0], " ", 3)[2]), &sent)
	if sent["id"] != float64(4) || sent["raft"] != "queen-3.h:7400" || sent["http"] != "queen-3.h:6632" {
		t.Fatalf("sent %v", sent)
	}

	_, err = cl.Promote(ctx, "queen-0", []int64{4})
	if RefusedCode(err) != "learner_behind" {
		t.Fatalf("%v", err)
	}
	_, err = cl.Remove(ctx, "queen-0", 5)
	if RefusedCode(err) != "no_quorum" {
		t.Fatalf("%v", err)
	}
	// An error that is not a refusal has no code.
	if RefusedCode(errors.New("connection refused")) != "" {
		t.Fatal("a transport error is not a refusal")
	}
}

func TestTheProbeProvesAWriteCommitted(t *testing.T) {
	cl, seen := serve(t, map[string][2]string{"PUT /api/v1/kv/queen-operator/probe": {"200", `{"applied":true,"index":0,"key":"probe","op":"put","version":1}`}})
	if err := cl.Probe(context.Background(), "queen-0"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains((*seen)[0], `"ttlSeconds":600`) {
		t.Fatalf("the probe must expire by itself: %s", (*seen)[0])
	}
	// A node without its quorum answers 503 once the write's deadline passed.
	cl, _ = serve(t, map[string][2]string{"PUT /api/v1/kv/queen-operator/probe": {"503", `{"error":"the request deadline elapsed","code":"timeout"}`}})
	if err := cl.Probe(context.Background(), "queen-0"); err == nil {
		t.Fatal("a write that did not commit is a failed probe")
	}
}
