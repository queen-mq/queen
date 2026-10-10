// Package broker is the part of the broker's HTTP API the operator uses: a
// node's health, the raft membership and its changes, and a write that proves
// the cluster commits.
package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Transport sends one request to the broker port of one pod and returns the
// status code and the body. A status the broker answered is not an error.
type Transport interface {
	Do(ctx context.Context, pod, method, path string, body []byte) (int, []byte, error)
}

type Client struct {
	T Transport
}

// Health is what one node says about itself (GET /health).
type Health struct {
	// 200: the node answered "healthy".
	Healthy bool   `json:"-"`
	Status  string `json:"status"`
	Version string `json:"version"`
	Raft    struct {
		Role    string `json:"role"`
		Leader  bool   `json:"leader"`
		Term    int64  `json:"term"`
		Applied int64  `json:"applied"`
		Commit  int64  `json:"commit"`
		Lag     int64  `json:"lag"`
		// How long ago the leader this node follows heard from a majority.
		// Absent before the broker reported it.
		QuorumAckMs *int64 `json:"quorumAckMs"`
		Apply       *struct {
			Failure json.RawMessage `json:"failure"`
		} `json:"apply"`
	} `json:"raft"`
}

// ApplyFailed says whether apply stopped on this node (a poisoned entry, a
// full or damaged disk): nothing the operator may repair.
func (h *Health) ApplyFailed() bool {
	if h.Raft.Apply == nil {
		return false
	}
	f := bytes.TrimSpace(h.Raft.Apply.Failure)
	return len(f) > 0 && !bytes.Equal(f, []byte("null"))
}

type Member struct {
	NodeID    int64  `json:"nodeId"`
	Voter     bool   `json:"voter"`
	Raft      string `json:"raft"`
	HTTP      string `json:"http"`
	Matched   *int64 `json:"matched"`
	Lag       *int64 `json:"lag"`
	LastAckMs *int64 `json:"lastAckMs"`
	Live      bool   `json:"live"`
}

// Membership is the raft membership as one node reports it
// (GET /api/v1/system/raft/membership).
type Membership struct {
	NodeID int64 `json:"nodeId"`
	// "leader": the leader's own view. "follower": this node's membership
	// with the figures of the last view the leader sent it.
	Source         string    `json:"source"`
	ViewAgeMs      *int64    `json:"viewAgeMs"`
	Leader         *int64    `json:"leader"`
	Term           int64     `json:"term"`
	Voters         []int64   `json:"voters"`
	Learners       []int64   `json:"learners"`
	Joint          [][]int64 `json:"joint"`
	ChangeInFlight bool      `json:"changeInFlight"`
	LastLogIndex   int64     `json:"lastLogIndex"`
	CommittedIndex int64     `json:"committedIndex"`
	PromoteMaxLag  int64     `json:"promoteMaxLag"`
	Members        []Member  `json:"members"`
}

func (m *Membership) Member(id int64) *Member {
	for i := range m.Members {
		if m.Members[i].NodeID == id {
			return &m.Members[i]
		}
	}
	return nil
}

func (m *Membership) IsVoter(id int64) bool   { return contains(m.Voters, id) }
func (m *Membership) IsLearner(id int64) bool { return contains(m.Learners, id) }
func (m *Membership) IsMember(id int64) bool  { return m.IsVoter(id) || m.IsLearner(id) }

func contains(ids []int64, id int64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

// Refused is a membership change the leader refused, with its stable code:
// in_flight, no_quorum, last_voter, learner_behind, address_mismatch,
// membership_changed, leader_changed, not_a_learner, timeout,
// leader_unreachable, ...
type Refused struct {
	Status int
	Code   string
	Reason string
}

func (r *Refused) Error() string {
	return fmt.Sprintf("membership change refused (%d %s): %s", r.Status, r.Code, r.Reason)
}

// RefusedCode is the code of a refused change, or "" for any other error.
func RefusedCode(err error) string {
	var r *Refused
	if errors.As(err, &r) {
		return r.Code
	}
	return ""
}

func (c *Client) Health(ctx context.Context, pod string) (*Health, error) {
	status, body, err := c.T.Do(ctx, pod, "GET", "/health", nil)
	if err != nil {
		return nil, err
	}
	var h Health
	if err := json.Unmarshal(body, &h); err != nil {
		return nil, fmt.Errorf("%s /health answered %d with a body that is not health: %w", pod, status, err)
	}
	h.Healthy = status == 200
	return &h, nil
}

func (c *Client) Membership(ctx context.Context, pod string) (*Membership, error) {
	status, body, err := c.T.Do(ctx, pod, "GET", "/api/v1/system/raft/membership", nil)
	if err != nil {
		return nil, err
	}
	return membershipAnswer(status, body)
}

// AddLearner adds node id, reachable at the two addresses, as a learner. The
// change is idempotent on the broker.
func (c *Client) AddLearner(ctx context.Context, via string, id int64, raftAddr, httpAddr string) (*Membership, error) {
	body, _ := json.Marshal(map[string]any{"id": id, "raft": raftAddr, "http": httpAddr})
	return c.change(ctx, via, "POST", "/api/v1/system/raft/membership/learners", body)
}

// Promote makes learners voters, in one change.
func (c *Client) Promote(ctx context.Context, via string, ids []int64) (*Membership, error) {
	body, _ := json.Marshal(map[string]any{"ids": ids})
	return c.change(ctx, via, "POST", "/api/v1/system/raft/membership/promote", body)
}

// Remove drops a voter or a learner from the membership.
func (c *Client) Remove(ctx context.Context, via string, id int64) (*Membership, error) {
	return c.change(ctx, via, "DELETE", fmt.Sprintf("/api/v1/system/raft/membership/members/%d", id), nil)
}

func (c *Client) change(ctx context.Context, via, method, path string, body []byte) (*Membership, error) {
	status, out, err := c.T.Do(ctx, via, method, path, body)
	if err != nil {
		return nil, err
	}
	return membershipAnswer(status, out)
}

func membershipAnswer(status int, body []byte) (*Membership, error) {
	var a struct {
		OK         *bool       `json:"ok"`
		Code       string      `json:"code"`
		Error      string      `json:"error"`
		Membership *Membership `json:"membership"`
	}
	if err := json.Unmarshal(body, &a); err != nil {
		return nil, fmt.Errorf("the membership route answered %d with a body that is not JSON: %s", status, clip(body))
	}
	if status != 200 || (a.OK != nil && !*a.OK) || a.Membership == nil {
		code := a.Code
		if code == "" {
			code = fmt.Sprintf("http_%d", status)
		}
		return nil, &Refused{Status: status, Code: code, Reason: a.Error}
	}
	return a.Membership, nil
}

// Probe writes one small key through pod and reports whether the write
// committed within the deadline of ctx. It is the only test that proves a
// cluster commits: a node can answer /health and hold a membership while
// nothing it takes ever commits.
func (c *Client) Probe(ctx context.Context, pod string) error {
	body, _ := json.Marshal(map[string]any{
		"value":      time.Now().UTC().Format(time.RFC3339),
		"ttlSeconds": 600,
	})
	status, out, err := c.T.Do(ctx, pod, "PUT", "/api/v1/kv/queen-operator/probe", body)
	if err != nil {
		return err
	}
	var a struct {
		Applied bool `json:"applied"`
	}
	if status != 200 || json.Unmarshal(out, &a) != nil || !a.Applied {
		return fmt.Errorf("the write probe through %s answered %d: %s", pod, status, clip(out))
	}
	return nil
}

func clip(b []byte) string {
	const max = 200
	if len(b) > max {
		return string(b[:max]) + "..."
	}
	return string(b)
}
