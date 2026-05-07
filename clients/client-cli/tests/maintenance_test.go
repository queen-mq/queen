package tests

import (
	"strings"
	"testing"
	"time"
)

// TestMaintenance_* mirror clients/client-js/test-v2/maintenance.js. The
// CLI surface is `queenctl maintenance get|on|off [--pop]`.
//
// IMPORTANT: maintenance mode is process-wide on the broker. Each test
// flips it back off in t.Cleanup so a failed assert doesn't leave the
// broker in a bad state for the rest of the suite.

// maintenanceState reads the get response and returns the global flag, the
// pop-scope flag, and the raw map (so individual tests can poke at extra
// fields). The endpoint shape is:
//
//	GET  /api/v1/system/maintenance      -> {"maintenanceMode":bool, "popMaintenanceMode":bool, ...}
//	GET  /api/v1/system/maintenance/pop  -> {"popMaintenanceMode":bool, ...}
func maintenanceState(t *testing.T, popScope bool) (global, pop bool, raw map[string]any) {
	t.Helper()
	args := []string{"maintenance", "get", "-o", "json"}
	if popScope {
		args = append(args, "--pop")
	}
	out := runOK(t, args...)
	if err := jsonDecode(out, &raw); err != nil {
		t.Fatalf("decode maintenance state: %v\nbody: %s", err, out)
	}
	if v, ok := raw["maintenanceMode"].(bool); ok {
		global = v
	}
	if v, ok := raw["popMaintenanceMode"].(bool); ok {
		pop = v
	}
	if v, ok := raw["enabled"].(bool); ok {
		// Some response shapes return enabled=bool when the request was
		// scoped (e.g. set-pop-maintenance returns {"enabled":true}).
		if popScope {
			pop = v
		} else {
			global = v
		}
	}
	return global, pop, raw
}

func TestMaintenance_GetReturnsCurrentState(t *testing.T) {
	t.Cleanup(func() { _, _, _ = run("maintenance", "off") })
	_, _, raw := maintenanceState(t, false)
	// At minimum the shape must include the global flag.
	if _, ok := raw["maintenanceMode"]; !ok {
		t.Errorf("maintenance get must include maintenanceMode field; got: %v", raw)
	}
}

// TestMaintenance_ToggleOnOff turns maintenance on and back off and
// verifies the get response reflects each step.
func TestMaintenance_ToggleOnOff(t *testing.T) {
	t.Cleanup(func() { _, _, _ = run("maintenance", "off") })
	runOK(t, "maintenance", "on", "--yes")
	if g, _, _ := maintenanceState(t, false); !g {
		t.Errorf("expected maintenanceMode=true after `maintenance on`")
	}
	runOK(t, "maintenance", "off")
	if g, _, _ := maintenanceState(t, false); g {
		t.Errorf("expected maintenanceMode=false after `maintenance off`")
	}
}

// TestMaintenance_OnRequiresYes verifies the destructive-confirmation guard.
func TestMaintenance_OnRequiresYes(t *testing.T) {
	t.Cleanup(func() { _, _, _ = run("maintenance", "off") })
	_, stderr, code := run("maintenance", "on")
	if code == 0 {
		t.Errorf("maintenance on without --yes should fail; stderr: %s", stderr)
	}
	if !strings.Contains(stderr, "--yes") {
		t.Errorf("expected --yes hint in stderr, got: %s", stderr)
	}
}

// TestMaintenance_PopScopeIsIndependent verifies that --pop affects only
// the pop-maintenance flag, not the global one.
func TestMaintenance_PopScopeIsIndependent(t *testing.T) {
	t.Cleanup(func() {
		_, _, _ = run("maintenance", "off")
		_, _, _ = run("maintenance", "off", "--pop")
	})
	runOK(t, "maintenance", "on", "--pop", "--yes")
	if _, p, _ := maintenanceState(t, true); !p {
		t.Errorf("expected popMaintenanceMode=true after `maintenance on --pop`")
	}
	if g, _, _ := maintenanceState(t, false); g {
		t.Errorf("--pop should not flip the global maintenanceMode")
	}
	runOK(t, "maintenance", "off", "--pop")
	if _, p, _ := maintenanceState(t, true); p {
		t.Errorf("expected popMaintenanceMode=false after `maintenance off --pop`")
	}
}

// TestMaintenance_PopScopeBlocksPops verifies the runtime effect of pop
// maintenance. Push a message, enable pop maintenance, attempt to pop -
// the broker must return zero messages even though the queue isn't empty.
// (This is the most user-visible effect of the flag.)
func TestMaintenance_PopScopeBlocksPops(t *testing.T) {
	t.Cleanup(func() { _, _, _ = run("maintenance", "off", "--pop") })

	q := uniqueQueue(t, "maint-pop-block")
	createQueue(t, q)
	pushOne(t, q, "", map[string]any{"v": 1})

	runOK(t, "maintenance", "on", "--pop", "--yes")
	got := popN(t, q, 5,
		"--cg", "ct-maint-pop", "--auto-ack",
		"--wait=false", "--timeout", "300ms",
	)
	if len(got) != 0 {
		t.Errorf("pop maintenance: pop returned %d, want 0", len(got))
	}

	// After turning pop-maintenance off, the message must be available again.
	runOK(t, "maintenance", "off", "--pop")
	// The broker reads the maintenance flag through a TTL cache; give it a
	// moment to refresh.
	time.Sleep(500 * time.Millisecond)
	got = popN(t, q, 5,
		"--cg", "ct-maint-pop", "--auto-ack", "--timeout", "5s",
	)
	if len(got) != 1 {
		t.Errorf("after maintenance off: got %d, want 1", len(got))
	}
}
