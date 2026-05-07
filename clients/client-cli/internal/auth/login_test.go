package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPasswordLoginSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/login":
			http.SetCookie(w, &http.Cookie{Name: "token", Value: "good-jwt", HttpOnly: true})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	tok, err := PasswordLogin(srv.URL, "alice", "secret", false)
	if err != nil {
		t.Fatal(err)
	}
	if tok != "good-jwt" {
		t.Errorf("got %q want good-jwt", tok)
	}
}

func TestPasswordLoginBadCreds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"error":"Invalid credentials"}`))
	}))
	defer srv.Close()
	_, err := PasswordLogin(srv.URL, "x", "y", false)
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}
}

func TestIsGoogleEnabled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/auth/config" {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"google":{"enabled":true}}`))
	}))
	defer srv.Close()
	ok, err := IsGoogleEnabled(srv.URL)
	if err != nil || !ok {
		t.Errorf("got (%v,%v)", ok, err)
	}
}

func TestGoogleAuthorizeURL(t *testing.T) {
	u, err := GoogleAuthorizeURL("https://example.com/proxy/")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(u, "/api/auth/google") {
		t.Errorf("unexpected URL: %s", u)
	}
}
