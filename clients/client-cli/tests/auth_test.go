package tests

import (
	"net/http/httptest"
	"net/http"
	"strings"
	"testing"
)

// TestAuth_* mirror clients/client-js/test-v2/auth.js + test_auth.py for
// the queenctl surface: --token / $QUEEN_TOKEN / config token-ref + the
// proxy login flow.
//
// We do NOT rely on a real proxy being live - there's no reliable way to
// stand one up inside the test process. Instead:
//
//   - --token plumbing is verified against an httptest fake that
//     accepts/rejects based on the Authorization header.
//   - login --method password is verified via the same fake speaking the
//     proxy's /api/login contract.
//   - --method google is verified by spinning up a fake /api/auth/config
//     and asserting the CLI surfaces "not configured" cleanly when the
//     proxy says google.enabled=false.

// TestAuth_TokenFlagAttachesBearer verifies that --token surfaces as
// Authorization: Bearer X on every server-bound request.
func TestAuth_TokenFlagAttachesBearer(t *testing.T) {
	gotAuth := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"healthy","database":"connected","version":"0.0.0-test"}`))
	}))
	defer srv.Close()
	stdout, _, code := runWith(runOpts{
		env:     []string{"QUEEN_TOKEN="},
		rawArgs: true,
	}, "ping", "--token", "secret-jwt", "--server", srv.URL)
	if code != 0 {
		t.Fatalf("ping exit %d, stdout: %s", code, stdout)
	}
	if gotAuth != "Bearer secret-jwt" {
		t.Errorf("server saw Authorization=%q, want %q", gotAuth, "Bearer secret-jwt")
	}
}

// TestAuth_TokenEnvAttachesBearer verifies $QUEEN_TOKEN is honored when no
// flag is given.
func TestAuth_TokenEnvAttachesBearer(t *testing.T) {
	gotAuth := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"healthy","database":"connected","version":"0.0.0-test"}`))
	}))
	defer srv.Close()
	stdout, _, code := runWith(runOpts{
		env:     []string{"QUEEN_TOKEN=env-jwt"},
		rawArgs: true,
	}, "ping", "--server", srv.URL)
	if code != 0 {
		t.Fatalf("ping exit %d, stdout: %s", code, stdout)
	}
	if gotAuth != "Bearer env-jwt" {
		t.Errorf("server saw Authorization=%q, want %q", gotAuth, "Bearer env-jwt")
	}
}

// TestAuth_LoginPasswordCapturesCookieJWT mirrors test_auth.py's
// password-flow scenario via the proxy.
func TestAuth_LoginPasswordCapturesCookieJWT(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/login":
			http.SetCookie(w, &http.Cookie{Name: "token", Value: "fake-jwt-from-proxy", HttpOnly: true})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cfg := newTempConfig(t)
	defer cfg.cleanup(t)
	cfg.setContext(t, "auth-test", srv.URL)

	stdout, stderr, code := runWith(runOpts{
		env:     []string{"QUEEN_CONFIG=" + cfg.path},
		rawArgs: true,
	},
		"--config", cfg.path,
		"login", "--method", "password",
		"-u", "alice", "--password", "shhh", "--context", "auth-test",
	)
	if code != 0 {
		t.Fatalf("login exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "logged in") {
		t.Errorf("expected 'logged in' confirmation, got: %s", stdout)
	}
}

// TestAuth_LoginPasswordRejectsBadCredentials maps proxy 401 to exit code 3
// (CodeAuth) at the CLI surface.
func TestAuth_LoginPasswordRejectsBadCredentials(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"Invalid credentials"}`))
	}))
	defer srv.Close()

	cfg := newTempConfig(t)
	defer cfg.cleanup(t)
	cfg.setContext(t, "bad-creds", srv.URL)

	_, _, code := runWith(runOpts{
		env:     []string{"QUEEN_CONFIG=" + cfg.path},
		rawArgs: true,
	},
		"--config", cfg.path,
		"login", "--method", "password",
		"-u", "alice", "--password", "wrong", "--context", "bad-creds",
	)
	if code != 3 {
		t.Errorf("bad credentials should exit 3, got %d", code)
	}
}

// TestAuth_LoginGoogleNotConfiguredFailsCleanly: when /api/auth/config
// reports google.enabled=false, login --method google must error out with
// a useful message, not crash.
func TestAuth_LoginGoogleNotConfiguredFailsCleanly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/auth/config" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"google":{"enabled":false}}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	cfg := newTempConfig(t)
	defer cfg.cleanup(t)
	cfg.setContext(t, "google-off", srv.URL)

	_, stderr, code := runWith(runOpts{
		env:     []string{"QUEEN_CONFIG=" + cfg.path},
		rawArgs: true,
	},
		"--config", cfg.path,
		"login", "--method", "google", "--no-browser",
		"--context", "google-off",
	)
	if code == 0 {
		t.Errorf("login --method google should fail when proxy disables google; stderr: %s", stderr)
	}
	if !strings.Contains(stderr, "Google auth is not enabled") &&
		!strings.Contains(stderr, "google") {
		t.Errorf("error message should mention google: %s", stderr)
	}
}
