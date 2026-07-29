// Package auth implements the queenctl login flows. They all converge on the
// same outcome - a bearer credential stored in the OS keychain that
// subsequent commands attach as Authorization: Bearer.
//
//	token    paste a JWT or a qk_ cluster API key (CI / external IdP)
//	password POST /auth/login on the proxy with email/password
//	google   open https://<proxy>/auth/google in a browser
//	github   open https://<proxy>/auth/github in a browser
//
// The endpoints are queen-proxy's: the human-identity surface is mounted
// under /auth (never /api/auth, which the proxy's data-plane gateway refuses
// fail-closed). /auth/login takes a form-encoded body, answers a good
// password with a 303 plus the session JWT in an httpOnly cookie, and
// re-renders its HTML login page on a bad one. That cookie is HTTP-only on
// the wire but fully visible to the client that initiated the request, so we
// read the JWT straight out of the jar; it outlives the short bearer minted
// by /auth/session-token, which is the name-independent fallback for when the
// proxy runs with a custom cookie name.
//
// The legacy Node proxy (POST /api/login, GET /api/auth/config) is still
// tried when the response to /auth/login proves we are not talking to a
// queen-proxy auth host, so one queenctl build serves both generations.
package auth

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Paths on the proxy. The /api/* pair is the legacy Node proxy's and is only
// reached through the fallback in PasswordLogin / IsProviderEnabled.
const (
	loginPath        = "/auth/login"
	sessionTokenPath = "/auth/session-token"
	legacyLoginPath  = "/api/login"
	legacyConfigPath = "/api/auth/config"
)

// OAuth providers the proxy mounts under /auth/<name>.
const (
	ProviderGoogle = "google"
	ProviderGitHub = "github"
)

// ErrInvalidCredentials is returned by PasswordLogin when the proxy rejects
// the email/password pair.
var ErrInvalidCredentials = errors.New("invalid credentials")

// ErrRateLimited is returned when the proxy's per-IP login throttle refuses
// the attempt (10 tries per minute by default).
var ErrRateLimited = errors.New("too many login attempts")

// errNotProxyAuth marks a response that cannot have come from queen-proxy's
// /auth/login, which is the signal to retry the legacy endpoint. Never
// surfaces on its own: PasswordLogin folds it into the legacy error.
var errNotProxyAuth = errors.New("not a queen-proxy auth host")

// Cookie names that may carry the session JWT, most likely first:
// queen-proxy's default (QUEEN_PROXY_COOKIE_NAME), then the legacy Node
// proxy's (COOKIE_NAME) and the name its earliest login page used.
var sessionCookieNames = []string{"queen_session", "queen_token", "token"}

// PasswordLogin signs in with user + password and returns the bearer to
// store. It tries queen-proxy's /auth/login first and falls back to the
// legacy Node proxy's /api/login. Returns ErrInvalidCredentials when the
// server rejects the pair so the CLI can map it to exit code 3.
func PasswordLogin(serverURL, user, password string, insecure bool) (string, error) {
	client, err := newClient(insecure)
	if err != nil {
		return "", err
	}
	token, err := proxyLogin(client, serverURL, user, password)
	if !errors.Is(err, errNotProxyAuth) {
		return token, err
	}
	token, legacyErr := legacyLogin(client, serverURL, user, password)
	if legacyErr != nil {
		// Keep the legacy error wrapped (the caller matches
		// ErrInvalidCredentials through it) and name the path we tried first,
		// so a misconfigured server URL is not reported as a bad password.
		return "", fmt.Errorf("%w (POST %s first: %v)", legacyErr, loginPath, err)
	}
	return token, nil
}

// proxyLogin runs the queen-proxy contract. Anything outside the three shapes
// that endpoint can produce - 3xx with a session, an HTML 401, a throttled
// 429 - yields errNotProxyAuth.
func proxyLogin(c *http.Client, serverURL, email, password string) (string, error) {
	endpoint, err := joinURL(serverURL, loginPath)
	if err != nil {
		return "", err
	}
	resp, err := c.PostForm(endpoint, url.Values{"email": {email}, "password": {password}})
	if err != nil {
		return "", fmt.Errorf("post login: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	switch {
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		if token := sessionJWT(c, endpoint, resp); token != "" {
			return token, nil
		}
		// A custom cookie name we could not recognise: ask for a bearer
		// minted from whatever session cookie the jar is now holding.
		token, terr := sessionToken(c, serverURL)
		if terr != nil {
			return "", fmt.Errorf("%w: redirect carried no session (%v)", errNotProxyAuth, terr)
		}
		return token, nil
	case resp.StatusCode == http.StatusUnauthorized && isHTML(resp.Header):
		// The proxy re-renders its login page on a bad password. A JSON 401
		// is the legacy proxy's cookie bouncer refusing an unknown route -
		// not a verdict on the credentials.
		return "", ErrInvalidCredentials
	case resp.StatusCode == http.StatusTooManyRequests:
		return "", rateLimited(resp.Header)
	}
	return "", fmt.Errorf("%w: HTTP %d: %s", errNotProxyAuth, resp.StatusCode, snippet(body))
}

// legacyLogin runs the Node proxy contract: JSON {username,password}, 200 and
// the JWT in a Set-Cookie.
func legacyLogin(c *http.Client, serverURL, username, password string) (string, error) {
	endpoint, err := joinURL(serverURL, legacyLoginPath)
	if err != nil {
		return "", err
	}
	bb, _ := json.Marshal(map[string]string{"username": username, "password": password})
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(bb))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return "", fmt.Errorf("post login: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode == http.StatusUnauthorized {
		return "", ErrInvalidCredentials
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("login failed: HTTP %d: %s", resp.StatusCode, snippet(body))
	}
	if token := sessionJWT(c, endpoint, resp); token != "" {
		return token, nil
	}
	return "", errors.New("login succeeded but no JWT cookie was returned")
}

// sessionToken exchanges the session cookie for the short bearer the proxy
// hands its SPA. Used when the session cookie itself could not be identified,
// and named in the browser-flow instructions.
func sessionToken(c *http.Client, serverURL string) (string, error) {
	endpoint, err := joinURL(serverURL, sessionTokenPath)
	if err != nil {
		return "", err
	}
	resp, err := c.Get(endpoint)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: HTTP %d", sessionTokenPath, resp.StatusCode)
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", errors.New("empty token")
	}
	return out.Token, nil
}

// sessionJWT picks the session JWT out of the jar, falling back to the raw
// Set-Cookie headers - the jar silently drops a cookie whose Domain does not
// match the URL we dialled, or a Secure one received over plain http, both of
// which happen when a cloud-configured proxy is reached directly.
func sessionJWT(c *http.Client, endpoint string, resp *http.Response) string {
	var cookies []*http.Cookie
	if u, err := url.Parse(endpoint); err == nil && c.Jar != nil {
		cookies = c.Jar.Cookies(u)
	}
	cookies = append(cookies, resp.Cookies()...)
	for _, name := range sessionCookieNames {
		for _, ck := range cookies {
			if ck.Name == name && ck.Value != "" {
				return ck.Value
			}
		}
	}
	// Custom QUEEN_PROXY_COOKIE_NAME: take the one shaped like a JWT.
	for _, ck := range cookies {
		if looksLikeJWT(ck.Value) {
			return ck.Value
		}
	}
	return ""
}

// AuthorizeURL is the URL to open in a browser to start provider's OAuth
// flow. `next` lands the browser on /auth/session-token after the callback,
// which renders the bearer the user pastes back: the session cookie is
// httpOnly, and the proxy only accepts same-origin relative `next` values, so
// a loopback capture is not available to the CLI.
func AuthorizeURL(serverURL, provider string) (string, error) {
	u, err := parseJoin(serverURL, authPath(provider))
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("next", sessionTokenPath)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// SessionTokenURL is where the browser lands after a successful OAuth
// callback; printed in the instructions so the user can reopen it.
func SessionTokenURL(serverURL string) (string, error) {
	return joinURL(serverURL, sessionTokenPath)
}

// IsProviderEnabled reports whether the proxy has provider's OAuth client
// configured. queen-proxy answers GET /auth/<provider> with a redirect to the
// provider's own authorize URL when it is configured and a 404
// {"code":"not_configured"} when it is not; nothing reaches the provider
// because the redirect is not followed. Any other shape means this is not a
// queen-proxy auth host, so google falls back to the legacy Node probe -
// github never existed there.
func IsProviderEnabled(serverURL, provider string, insecure bool) (bool, error) {
	c, err := newClient(insecure)
	if err != nil {
		return false, err
	}
	endpoint, err := joinURL(serverURL, authPath(provider))
	if err != nil {
		return false, err
	}
	resp, err := c.Get(endpoint)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	switch {
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		// Both providers live on https. A legacy proxy bounces unknown paths
		// to its own relative /login, which is not an OAuth start.
		if strings.HasPrefix(resp.Header.Get("Location"), "https://") {
			return true, nil
		}
	case resp.StatusCode == http.StatusNotFound && errorCode(body) == "not_configured":
		return false, nil
	case resp.StatusCode >= 500:
		// Auth-host mode without QUEEN_PROXY_PUBLIC_URL lands here: a proxy
		// misconfiguration the operator must fix, not "provider disabled".
		return false, fmt.Errorf("proxy cannot start the %s flow: HTTP %d: %s",
			provider, resp.StatusCode, snippet(body))
	}
	if provider != ProviderGoogle {
		return false, nil
	}
	return legacyGoogleEnabled(c, serverURL)
}

// legacyGoogleEnabled probes the Node proxy's /api/auth/config.
func legacyGoogleEnabled(c *http.Client, serverURL string) (bool, error) {
	endpoint, err := joinURL(serverURL, legacyConfigPath)
	if err != nil {
		return false, err
	}
	resp, err := c.Get(endpoint)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, nil
	}
	var cfg struct {
		Google struct {
			Enabled bool `json:"enabled"`
		} `json:"google"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&cfg); err != nil {
		return false, err
	}
	return cfg.Google.Enabled, nil
}

// newClient builds the login HTTP client: a cookie jar (the session JWT
// arrives as a Set-Cookie) and no redirect following, so the 303 that answers
// a good password is observed here instead of being chased into the console.
func newClient(insecure bool) (*http.Client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	c := &http.Client{
		Jar:     jar,
		Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	if insecure {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // --insecure
		c.Transport = tr
	}
	return c, nil
}

func authPath(provider string) string { return "/auth/" + provider }

// rateLimited turns the proxy's 429 into an error naming the wait, from the
// Retry-After header it always sets on a throttled login.
func rateLimited(h http.Header) error {
	if s, err := strconv.Atoi(strings.TrimSpace(h.Get("Retry-After"))); err == nil && s > 0 {
		return fmt.Errorf("%w; retry in %ds", ErrRateLimited, s)
	}
	return ErrRateLimited
}

// errorCode reads the machine-readable "code" from the proxy's JSON error
// envelope. Empty when the body is not such an object.
func errorCode(body []byte) string {
	var e struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		return ""
	}
	return e.Code
}

func isHTML(h http.Header) bool {
	return strings.HasPrefix(strings.ToLower(h.Get("Content-Type")), "text/html")
}

// looksLikeJWT reports the compact JWS shape: three non-empty base64url
// segments. Enough to pick the session out of a jar when the proxy runs with
// a cookie name we do not know.
func looksLikeJWT(v string) bool {
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return false
	}
	for _, p := range parts {
		if p == "" {
			return false
		}
		for _, r := range p {
			switch {
			case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			default:
				return false
			}
		}
	}
	return true
}

// snippet trims a response body down to something printable in one error line.
func snippet(body []byte) string {
	s := strings.TrimSpace(string(body))
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}

func parseJoin(base, path string) (*url.URL, error) {
	if base == "" {
		return nil, errors.New("server URL is empty")
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	return u, nil
}

func joinURL(base, path string) (string, error) {
	u, err := parseJoin(base, path)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
