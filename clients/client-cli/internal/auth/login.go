// Package auth implements the queenctl login flows. It supports three
// methods that all converge on the same outcome - a JWT stored in the OS
// keychain that subsequent commands attach as Authorization: Bearer.
//
//	token    paste a JWT (CI / external IdP)
//	password POST /api/login on the proxy with username/password
//	google   open https://<proxy>/api/auth/google in a browser; user pastes
//	         the JWT from the resulting dashboard page or browser cookie
//
// The proxy's login endpoint sets an HTTP-only cookie named "token". We
// scrape that Set-Cookie header (the cookie is HTTP-only on the wire but
// fully visible to the client that initiated the request) and persist the
// JWT.
package auth

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

// PasswordLogin posts username/password to the proxy at server and returns
// the JWT extracted from the Set-Cookie header. Returns a typed error when
// credentials are bad so the CLI can map it to exit code 3.
func PasswordLogin(serverURL, username, password string, insecure bool) (string, error) {
	endpoint, err := joinURL(serverURL, "/api/login")
	if err != nil {
		return "", err
	}
	body := map[string]string{"username": username, "password": password}
	bb, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(bb))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	jar, _ := cookiejar.New(nil)
	client := &http.Client{
		Jar:     jar,
		Timeout: 30 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("post login: %w", err)
	}
	defer resp.Body.Close()
	body2, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		return "", ErrInvalidCredentials
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("login failed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body2)))
	}
	// The token cookie is the JWT we want.
	u, _ := url.Parse(endpoint)
	for _, c := range jar.Cookies(u) {
		if c.Name == "token" {
			return c.Value, nil
		}
	}
	for _, raw := range resp.Header["Set-Cookie"] {
		for _, part := range strings.Split(raw, ";") {
			part = strings.TrimSpace(part)
			if eq := strings.IndexByte(part, '='); eq > 0 && part[:eq] == "token" {
				return part[eq+1:], nil
			}
		}
	}
	return "", errors.New("login succeeded but no JWT cookie was returned")
}

// ErrInvalidCredentials is returned by PasswordLogin when the proxy
// responds with 401.
var ErrInvalidCredentials = errors.New("invalid credentials")

// GoogleAuthorizeURL returns the URL the user should open in a browser to
// kick off the proxy's Google OAuth flow.
func GoogleAuthorizeURL(serverURL string) (string, error) {
	return joinURL(serverURL, "/api/auth/google")
}

// IsGoogleEnabled probes /api/auth/config and returns whether the proxy is
// configured for Google login.
func IsGoogleEnabled(serverURL string) (bool, error) {
	endpoint, err := joinURL(serverURL, "/api/auth/config")
	if err != nil {
		return false, err
	}
	resp, err := http.Get(endpoint)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false, nil
	}
	var cfg struct {
		Google struct {
			Enabled bool `json:"enabled"`
		} `json:"google"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return false, err
	}
	return cfg.Google.Enabled, nil
}

func joinURL(base, path string) (string, error) {
	if base == "" {
		return "", errors.New("server URL is empty")
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	return u.String(), nil
}
