// Package runtime hosts the streaming engine's HTTP client, register/cycle/
// state helpers, and the main Runner loop. Mirror of the JS streams/runtime.
package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/streams/util"
)

// HTTPError carries the HTTP status + parsed body for failed POSTs.
type HTTPError struct {
	Status int
	Body   map[string]interface{}
	Msg    string
}

func (e *HTTPError) Error() string { return e.Msg }

// TokenSource is where the bearer token comes from when it is not a fixed
// string. The queen client's (Queen.TokenSource) is one: given to a runner, it
// serves the client and the runner with one fetch per refresh.
type TokenSource interface {
	// Token answers the token to send. failed is the token a request was just
	// refused with (401): a new one is fetched unless another request already
	// replaced it. "" asks for the current token.
	Token(ctx context.Context, failed string) (string, error)
}

// HTTPClient is the thin streams /streams/v1/* client.
type HTTPClient struct {
	URL         string
	BearerToken string
	// TokenSource, when set, gives the token instead of BearerToken: asked on
	// every attempt, and once more after a 401, when the request is sent again.
	TokenSource   TokenSource
	TimeoutMs     int
	RetryAttempts int
	hc            *http.Client
}

// NewHTTPClient creates a client. timeoutMs default 30s, retries default 3.
func NewHTTPClient(url, bearer string, timeoutMs, retries int) *HTTPClient {
	if url == "" {
		panic("HTTPClient requires url")
	}
	if timeoutMs == 0 {
		timeoutMs = 30000
	}
	if retries == 0 {
		retries = 3
	}
	return &HTTPClient{
		URL:           strings.TrimRight(url, "/"),
		BearerToken:   bearer,
		TimeoutMs:     timeoutMs,
		RetryAttempts: retries,
		hc:            &http.Client{Timeout: time.Duration(timeoutMs) * time.Millisecond},
	}
}

// Post issues a POST with retries on 5xx + 408 + 429 + transport errors.
func (c *HTTPClient) Post(ctx context.Context, path string, body interface{}) (map[string]interface{}, error) {
	full := c.URL + path
	bo := util.NewBackoff(100, 5000, 2)
	var lastErr error
	refreshed := false
	for attempt := 0; attempt <= c.RetryAttempts; attempt++ {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, full, bytes.NewReader(buf))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		token := c.BearerToken
		if c.TokenSource != nil {
			// A source that gives no token fails the call: nothing is sent.
			if token, err = c.TokenSource.Token(ctx, ""); err != nil {
				return nil, err
			}
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := c.hc.Do(req)
		if err != nil {
			lastErr = err
			if attempt < c.RetryAttempts {
				time.Sleep(bo.Next())
				continue
			}
			return nil, err
		}
		bodyB, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		var parsed map[string]interface{}
		if len(bodyB) > 0 {
			if err := json.Unmarshal(bodyB, &parsed); err != nil {
				parsed = map[string]interface{}{"raw": string(bodyB)}
			}
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return parsed, nil
		}
		errMsg := fmt.Sprintf("HTTP %d", resp.StatusCode)
		if e, ok := parsed["error"].(string); ok {
			errMsg = e
		}
		he := &HTTPError{Status: resp.StatusCode, Body: parsed, Msg: errMsg}
		// A token the broker no longer takes: one new one, and the call once
		// more. It counts as no attempt; a second 401 is the caller's.
		if resp.StatusCode == http.StatusUnauthorized && c.TokenSource != nil && !refreshed {
			refreshed = true
			if _, err := c.TokenSource.Token(ctx, token); err != nil {
				return nil, err
			}
			attempt--
			continue
		}
		// 4xx (other than 408/429) is not retriable.
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != 408 && resp.StatusCode != 429 {
			return nil, he
		}
		lastErr = he
		if attempt < c.RetryAttempts {
			time.Sleep(bo.Next())
			continue
		}
		return nil, he
	}
	return nil, lastErr
}
