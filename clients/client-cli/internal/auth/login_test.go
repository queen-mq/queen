package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// proxyLoginHandler fakes queen-proxy's /auth/login: a form-encoded body, a
// 303 + session cookie on the good pair, and the re-rendered HTML login page
// on the bad one.
func proxyLoginHandler(t *testing.T, wantEmail, wantPassword, cookieName, jwt string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/login":
			if err := r.ParseForm(); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.PostForm.Get("email") != wantEmail || r.PostForm.Get("password") != wantPassword {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte("<html>Invalid email or password.</html>"))
				return
			}
			http.SetCookie(w, &http.Cookie{Name: cookieName, Value: jwt, Path: "/", HttpOnly: true})
			w.Header().Set("Location", "/")
			w.WriteHeader(http.StatusSeeOther)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func TestPasswordLoginProxy(t *testing.T) {
	srv := httptest.NewServer(proxyLoginHandler(t, "alice@example.com", "secret", "queen_session", "a.b.c"))
	defer srv.Close()

	tok, err := PasswordLogin(srv.URL, "alice@example.com", "secret", false)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "a.b.c" {
		t.Errorf("got %q want a.b.c", tok)
	}
}

// A custom QUEEN_PROXY_COOKIE_NAME still yields the session: the JWT shape
// identifies it, and /auth/session-token backs that up.
func TestPasswordLoginProxyCustomCookieName(t *testing.T) {
	srv := httptest.NewServer(proxyLoginHandler(t, "alice@example.com", "secret", "sid", "x.y.z"))
	defer srv.Close()

	tok, err := PasswordLogin(srv.URL, "alice@example.com", "secret", false)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "x.y.z" {
		t.Errorf("got %q want x.y.z", tok)
	}
}

// When no cookie is recognisable at all, the documented exchange endpoint is
// the fallback.
func TestPasswordLoginProxyFallsBackToSessionToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/login":
			http.SetCookie(w, &http.Cookie{Name: "sid", Value: "opaque-session-id", Path: "/"})
			w.Header().Set("Location", "/")
			w.WriteHeader(http.StatusSeeOther)
		case "/auth/session-token":
			if _, err := r.Cookie("sid"); err != nil {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"token":"short.lived.bearer","expires_in":900}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	tok, err := PasswordLogin(srv.URL, "alice@example.com", "secret", false)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "short.lived.bearer" {
		t.Errorf("got %q want short.lived.bearer", tok)
	}
}

func TestPasswordLoginProxyBadCreds(t *testing.T) {
	srv := httptest.NewServer(proxyLoginHandler(t, "alice@example.com", "secret", "queen_session", "a.b.c"))
	defer srv.Close()

	_, err := PasswordLogin(srv.URL, "alice@example.com", "wrong", false)
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}
}

func TestPasswordLoginProxyThrottled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "42")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"too many login attempts","code":"rate_limited"}`))
	}))
	defer srv.Close()

	_, err := PasswordLogin(srv.URL, "alice@example.com", "secret", false)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited, got %v", err)
	}
	if !strings.Contains(err.Error(), "42") {
		t.Errorf("error should carry Retry-After: %v", err)
	}
}

// --- legacy Node proxy fallback -------------------------------------------

func TestPasswordLoginLegacyFallback(t *testing.T) {
	var sawAuthLogin bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/login":
			// The legacy proxy has no such route: its cookie bouncer bounces
			// the unauthenticated request to the login page.
			sawAuthLogin = true
			w.Header().Set("Location", "/login")
			w.WriteHeader(http.StatusFound)
		case "/api/login":
			http.SetCookie(w, &http.Cookie{Name: "queen_token", Value: "legacy-jwt", HttpOnly: true})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	tok, err := PasswordLogin(srv.URL, "alice", "secret", false)
	if err != nil {
		t.Fatal(err)
	}
	if !sawAuthLogin {
		t.Error("the new endpoint should be tried first")
	}
	if tok != "legacy-jwt" {
		t.Errorf("got %q want legacy-jwt", tok)
	}
}

// A JSON 401 at /auth/login is the legacy bouncer, not a bad password: the
// fallback must still run.
func TestPasswordLoginJSON401FallsBack(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/login":
			http.SetCookie(w, &http.Cookie{Name: "queen_token", Value: "legacy-jwt", HttpOnly: true})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"Authentication required"}`))
		}
	}))
	defer srv.Close()

	tok, err := PasswordLogin(srv.URL, "alice", "secret", false)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "legacy-jwt" {
		t.Errorf("got %q want legacy-jwt", tok)
	}
}

func TestPasswordLoginLegacyBadCreds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"Invalid credentials"}`))
	}))
	defer srv.Close()

	_, err := PasswordLogin(srv.URL, "x", "y", false)
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}
}

// --- provider probes -------------------------------------------------------

func TestIsProviderEnabledProxy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/google":
			w.Header().Set("Location", "https://accounts.google.com/o/oauth2/v2/auth?client_id=x")
			w.WriteHeader(http.StatusFound)
		case "/auth/github":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"github login not configured","code":"not_configured"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	ok, err := IsProviderEnabled(srv.URL, ProviderGoogle, false)
	if err != nil || !ok {
		t.Errorf("google: got (%v,%v), want (true,nil)", ok, err)
	}
	ok, err = IsProviderEnabled(srv.URL, ProviderGitHub, false)
	if err != nil || ok {
		t.Errorf("github: got (%v,%v), want (false,nil)", ok, err)
	}
}

func TestIsProviderEnabledLegacyFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/auth/config":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"google":{"enabled":true}}`))
		default:
			// Legacy bouncer: relative redirect, not an OAuth start.
			w.Header().Set("Location", "/login")
			w.WriteHeader(http.StatusFound)
		}
	}))
	defer srv.Close()

	ok, err := IsProviderEnabled(srv.URL, ProviderGoogle, false)
	if err != nil || !ok {
		t.Errorf("got (%v,%v), want (true,nil)", ok, err)
	}
}

func TestIsProviderEnabledSurfacesMisconfiguration(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"public base url required (auth-host mode)","code":"internal"}`))
	}))
	defer srv.Close()

	if _, err := IsProviderEnabled(srv.URL, ProviderGoogle, false); err == nil {
		t.Error("a 500 from the provider start must be surfaced, not read as disabled")
	}
}

func TestAuthorizeURL(t *testing.T) {
	u, err := AuthorizeURL("https://example.com/proxy/", ProviderGoogle)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(u, "https://example.com/proxy/auth/google?") {
		t.Errorf("unexpected URL: %s", u)
	}
	// next must land the browser on the bearer document.
	if !strings.Contains(u, "next=%2Fauth%2Fsession-token") {
		t.Errorf("authorize URL should carry next=/auth/session-token: %s", u)
	}
}

func TestLooksLikeJWT(t *testing.T) {
	if !looksLikeJWT("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.sig-_x") {
		t.Error("compact JWS should match")
	}
	for _, bad := range []string{"", "a.b", "a.b.c.d", "a..c", "opaque-session-id", "a.b.c!"} {
		if looksLikeJWT(bad) {
			t.Errorf("%q should not look like a JWT", bad)
		}
	}
}
