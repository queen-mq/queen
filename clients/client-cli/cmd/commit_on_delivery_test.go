package cmd

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// --commit-on-delivery, queenctl side.
//
// A pop that commits on delivery is autoAck=true on the pop request: the broker
// moves the group's cursor past the messages as it hands them out, with no
// lease and nothing to ack. `pop` and `ephemeral pop` call it
// --commit-on-delivery and keep --auto-ack as a hidden, deprecated alias.
// `tail` runs the consume loop, where --auto-ack is the client's ack after each
// printed message and never reaches the wire, so it keeps that name.

// requestsTo returns the recorded requests whose path is exactly path.
func (fb *fakeBroker) requestsTo(path string) []recordedRequest {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	var out []recordedRequest
	for _, r := range fb.reqs {
		if r.Path == path {
			out = append(out, r)
		}
	}
	return out
}

func TestPopSendsAutoAckOnlyWithCommitOnDelivery(t *testing.T) {
	cases := []struct {
		flags []string
		want  string
	}{
		{[]string{"--commit-on-delivery"}, "true"},
		{[]string{"--auto-ack"}, "true"}, // the deprecated alias, same effect
		{nil, ""},
	}
	for _, tc := range cases {
		fb := newFakeBroker(t, popBody("orders", ""))
		args := append([]string{"pop", "orders", "--cg", "billing", "--wait=false"}, tc.flags...)
		if err := runCLI(t, fb.url(), args...); err != nil {
			t.Fatalf("queenctl %s: %v", strings.Join(args, " "), err)
		}
		got := fb.firstPop(t).Query.Get("autoAck")
		if tc.want == "true" && got != "true" {
			t.Errorf("%v: autoAck = %q, want \"true\"", tc.flags, got)
		}
		if tc.want == "" && got == "true" {
			t.Errorf("%v: a pop without --commit-on-delivery must stay leased, sent autoAck=true", tc.flags)
		}
	}
}

func TestEphemeralPopSendsAutoAckWithCommitOnDelivery(t *testing.T) {
	t.Cleanup(func() {
		ephPopCommitOnDelivery, ephPopAutoAck = false, false
	})
	for _, flag := range []string{"--commit-on-delivery", "--auto-ack"} {
		ephPopCommitOnDelivery, ephPopAutoAck = false, false
		fb := newFakeBroker(t)
		fb.route("/api/v1/ephemeral/pop", `{"queue":"inbox","messages":[]}`)
		// An empty pop exits 4 ("no messages"); only the request matters here.
		_ = runCLI(t, fb.url(), "ephemeral", "pop", "inbox", flag)

		reqs := fb.requestsTo("/api/v1/ephemeral/pop")
		if len(reqs) != 1 {
			t.Fatalf("%s: expected 1 ephemeral pop, got %d", flag, len(reqs))
		}
		if got := reqs[0].Query.Get("autoAck"); got != "true" {
			t.Errorf("%s: autoAck = %q, want \"true\"", flag, got)
		}
	}
}

// --auto-ack on a pop is the old name: it still works, but help does not show
// it and using it says which flag replaced it.
func TestPopAutoAckIsAHiddenDeprecatedAlias(t *testing.T) {
	for name, c := range map[string]*cobra.Command{"pop": popCmd, "ephemeral pop": ephemeralPopCmd} {
		f := c.Flags().Lookup("auto-ack")
		if f == nil {
			t.Fatalf("%s must keep --auto-ack as an alias", name)
		}
		if !f.Hidden {
			t.Errorf("%s --auto-ack must be hidden from help", name)
		}
		if !strings.Contains(f.Deprecated, "--commit-on-delivery") {
			t.Errorf("%s --auto-ack must be deprecated in favour of --commit-on-delivery, got %q", name, f.Deprecated)
		}
	}
}

// tail runs the consume loop: --auto-ack is the client's ack after it prints a
// message, not an ack on the broker's side, and it is not deprecated.
func TestTailAutoAckHelpSaysTheClientAcksAfterPrinting(t *testing.T) {
	f := tailCmd.Flags().Lookup("auto-ack")
	if f == nil {
		t.Fatal("tail must keep --auto-ack")
	}
	if strings.Contains(f.Usage, "server-side") || !strings.Contains(f.Usage, "after printing") {
		t.Errorf("tail --auto-ack help = %q, want the client's ack after printing", f.Usage)
	}
	if f.Hidden || f.Deprecated != "" {
		t.Errorf("tail --auto-ack is the consume ack and stays a visible flag")
	}
}
