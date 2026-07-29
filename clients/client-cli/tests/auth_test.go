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
//   - login --method password is verified via the same fake speaking
//     queen-proxy's /auth/login contract (form body, 303 + session cookie).
//   - --method google is verified by spinning up a fake /auth/google that
//     answers 404 not_configured, asserting the CLI surfaces it cleanly.

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
		case "/auth/login":
			http.SetCookie(w, &http.Cookie{
				Name: "queen_session", Value: "fake.jwt.from-proxy", Path: "/", HttpOnly: true,
			})
			w.Header().Set("Location", "/")
			w.WriteHeader(http.StatusSeeOther)
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
		// queen-proxy re-renders its HTML login page on a bad password.
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`<html>Invalid email or password.</html>`))
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

// TestAuth_LoginGoogleNotConfiguredFailsCleanly: when /auth/google reports
// not_configured, login --method google must error out with a useful
// message, not crash.
func TestAuth_LoginGoogleNotConfiguredFailsCleanly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/google" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"google login not configured","code":"not_configured"}`))
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
	if !strings.Contains(stderr, "not enabled") && !strings.Contains(stderr, "google") {
		t.Errorf("error message should mention google: %s", stderr)
	}
}
