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

	"github.com/smartpricing/queen/client-go/streams/util"
)

// HTTPError carries the HTTP status + parsed body for failed POSTs.
type HTTPError struct {
	Status int
	Body   map[string]interface{}
	Msg    string
}

func (e *HTTPError) Error() string { return e.Msg }

// HTTPClient is the thin streams /streams/v1/* client.
type HTTPClient struct {
	URL           string
	BearerToken   string
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
		if c.BearerToken != "" {
			req.Header.Set("Authorization", "Bearer "+c.BearerToken)
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
