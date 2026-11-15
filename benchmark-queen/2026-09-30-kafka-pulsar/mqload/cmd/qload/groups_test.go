package main

import (
	"net/url"
	"strings"
	"testing"

	"mqload/internal/core"
)

func TestGroupsAndWherePath(t *testing.T) {
	q := &queenFlags{urls: "http://a", groupWhere: "data.k < 30; data.k >= 30 and data.k < 60;"}
	cfg := testCfg()
	if err := q.finalize(cfg); err != nil {
		t.Fatal(err)
	}
	if q.groups != 3 {
		t.Fatalf("groups %d, want 3 (one a where)", q.groups)
	}
	for i, want := range []struct{ g, w string }{{"g0", "data.k < 30"}, {"g1", "data.k >= 30 and data.k < 60"}, {"g2", ""}, {"g0", "data.k < 30"}} {
		if g, w := q.group(i); g != want.g || w != want.w {
			t.Fatalf("consumer %d: %q %q, want %q %q", i, g, w, want.g, want.w)
		}
	}
	g, w := q.group(1)
	p := popReq{queue: "b1", group: g, where: w, batch: 10, wait: true, timeoutMs: 100}.path()
	u, err := url.Parse(p)
	if err != nil {
		t.Fatal(err)
	}
	if u.Query().Get("where") != "data.k >= 30 and data.k < 60" || u.Query().Get("consumerGroup") != "g1" {
		t.Fatalf("path %s", p)
	}
	if strings.Contains(popReq{queue: "b1", batch: 1}.path(), "where") {
		t.Fatal("a queue-mode pop must not send where")
	}
	if b := string(ackBody([]qmsg{{TransactionID: "t", PartitionID: "1", LeaseID: "l"}}, "g1")); !strings.Contains(b, `"consumerGroup":"g1"`) || !strings.Contains(b, `"leaseId":"l"`) {
		t.Fatalf("ack body %s", b)
	}
	if err := (&queenFlags{urls: "http://a", groups: 1, groupWhere: "a;b"}).finalize(testCfg()); err == nil {
		t.Fatal("two wheres for one group must be refused")
	}
	q0 := &queenFlags{urls: "http://a"}
	if err := q0.finalize(testCfg()); err != nil {
		t.Fatal(err)
	}
	if g, w := q0.group(5); g != "" || w != "" {
		t.Fatalf("no -groups = queue mode, got %q %q", g, w)
	}
}

func testCfg() *core.Config { return &core.Config{} }
