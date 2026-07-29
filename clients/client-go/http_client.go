package queen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HttpClient handles HTTP requests with retry logic and load balancing.
type HttpClient struct {
	loadBalancer *LoadBalancer
	config       ClientConfig
	client       *http.Client
}

// NewHttpClient creates a new HTTP client.
func NewHttpClient(config ClientConfig) (*HttpClient, error) {
	// Validate and normalize URLs
	var urls []string
	if config.URL != "" {
		urls = []string{config.URL}
	} else if len(config.URLs) > 0 {
		urls = config.URLs
	} else {
		return nil, fmt.Errorf("at least one URL is required")
	}

	validatedURLs, err := ValidateURLs(urls)
	if err != nil {
		return nil, err
	}

	// Apply defaults
	if config.TimeoutMillis == 0 {
		config.TimeoutMillis = ClientDefaults.TimeoutMillis
	}
	if config.RetryAttempts == 0 {
		config.RetryAttempts = ClientDefaults.RetryAttempts
	}
	if config.RetryDelayMillis == 0 {
		config.RetryDelayMillis = ClientDefaults.RetryDelayMillis
	}
	if config.LoadBalancingStrategy == "" {
		config.LoadBalancingStrategy = ClientDefaults.LoadBalancingStrategy
	}
	if config.AffinityHashRing == 0 {
		config.AffinityHashRing = ClientDefaults.AffinityHashRing
	}
	if config.HealthRetryAfterMillis == 0 {
		config.HealthRetryAfterMillis = ClientDefaults.HealthRetryAfterMillis
	}

	// Create load balancer
	lb := NewLoadBalancer(
		validatedURLs,
		LoadBalancerStrategy(config.LoadBalancingStrategy),
		config.AffinityHashRing,
		config.HealthRetryAfterMillis,
		config.EnableFailover,
	)

	// Create HTTP client with a tuned transport. Go's default
	// MaxIdleConnsPerHost is 2, which throttles high-concurrency single-host
	// workloads (load generators, busy producer pools) by forcing constant
	// connection churn. Default to a generous idle pool; allow explicit override.
	maxIdlePerHost := config.MaxIdleConnsPerHost
	if maxIdlePerHost <= 0 {
		maxIdlePerHost = 256
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = maxIdlePerHost
	transport.MaxIdleConns = maxIdlePerHost * 4
	if config.MaxConnsPerHost > 0 {
		transport.MaxConnsPerHost = config.MaxConnsPerHost
	}

	httpClient := &http.Client{
		Timeout:   time.Duration(config.TimeoutMillis) * time.Millisecond,
		Transport: transport,
	}

	return &HttpClient{
		loadBalancer: lb,
		config:       config,
		client:       httpClient,
	}, nil
}

// RequestOption configures per-request behavior, currently just which 429
// retry policy applies (see Retry429Config).
type RequestOption func(*requestOptions)

type requestOptions struct {
	// retryKind is "" (bounded, push-like default) or "pop" (long-poll,
	// unbounded backoff by default). See HttpClient.retry429PolicyFor.
	retryKind string
}

// WithLongPollRetry marks a request as a long-poll pop (wait=true): on a
// 429, it backs off indefinitely by default instead of the bounded
// push-like attempt budget, matching PLAN_QUEEN_PROXY_CLOUD.md §4/§9
// (client 429 backoff, blocker B4) -- the outer poll loop is already
// unbounded, so an individual 429 should be waited out, not surfaced as a
// failure after a handful of tries.
func WithLongPollRetry() RequestOption {
	return func(o *requestOptions) { o.retryKind = "pop" }
}

func resolveRequestOptions(opts []RequestOption) requestOptions {
	ro := requestOptions{}
	for _, o := range opts {
		o(&ro)
	}
	return ro
}

// Get performs a GET request with retry logic.
func (hc *HttpClient) Get(ctx context.Context, path string, timeoutMs int, affinityKey string, opts ...RequestOption) (map[string]interface{}, error) {
	return hc.doRequest(ctx, http.MethodGet, path, nil, timeoutMs, affinityKey, resolveRequestOptions(opts))
}

// Post performs a POST request with retry logic.
func (hc *HttpClient) Post(ctx context.Context, path string, body interface{}, opts ...RequestOption) (map[string]interface{}, error) {
	return hc.doRequest(ctx, http.MethodPost, path, body, 0, "", resolveRequestOptions(opts))
}

// PostWithAffinity performs a POST request with affinity key.
func (hc *HttpClient) PostWithAffinity(ctx context.Context, path string, body interface{}, affinityKey string, opts ...RequestOption) (map[string]interface{}, error) {
	return hc.doRequest(ctx, http.MethodPost, path, body, 0, affinityKey, resolveRequestOptions(opts))
}

// Delete performs a DELETE request with retry logic.
func (hc *HttpClient) Delete(ctx context.Context, path string, opts ...RequestOption) (map[string]interface{}, error) {
	return hc.doRequest(ctx, http.MethodDelete, path, nil, 0, "", resolveRequestOptions(opts))
}

// doRequest performs an HTTP request with retry logic: the outer loop here
// retries 5xx/network failures across backends (existing RetryAttempts
// behavior, unchanged); each individual attempt is delegated to
// doRequestWithRetry429, which transparently retries HTTP 429 responses
// in place (same backend, backoff-paced) before returning.
func (hc *HttpClient) doRequest(ctx context.Context, method, path string, body interface{}, timeoutMs int, affinityKey string, ro requestOptions) (map[string]interface{}, error) {
	var lastErr error

	// Use custom timeout if provided
	timeout := hc.config.TimeoutMillis
	if timeoutMs > 0 {
		timeout = timeoutMs
	}

	// A negative RetryAttempts means "no retries". The zero value is coerced to
	// the default in NewHttpClient, so a negative sentinel is the only way for a
	// caller to request exactly one attempt. Clamp here so the loop runs once.
	maxRetries := hc.config.RetryAttempts
	if maxRetries < 0 {
		maxRetries = 0
	}
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Get URL from load balancer
		baseURL := hc.loadBalancer.GetURL(affinityKey)
		if baseURL == "" {
			return nil, fmt.Errorf("no available servers")
		}

		url := baseURL + path

		logDebug("HttpClient.doRequest", map[string]interface{}{
			"method":  method,
			"url":     url,
			"attempt": attempt,
		})

		result, err := hc.doRequestWithRetry429(ctx, method, url, body, timeout, ro)
		if err == nil {
			hc.loadBalancer.MarkHealthy(baseURL)
			return result, nil
		}
		lastErr = err

		// Check if context was cancelled
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if httpErr, ok := err.(*HTTPError); ok {
			// 4xx (including a 429 whose retry429 policy is exhausted, and a
			// terminal 403) is never retried here, and never fails over to a
			// different backend -- it isn't a backend-health signal.
			if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 {
				logError("HttpClient.doRequest", map[string]interface{}{
					"status":  httpErr.StatusCode,
					"code":    httpErr.Code,
					"body":    httpErr.Body,
					"noRetry": true,
				})
				return nil, httpErr
			}
			// Mark unhealthy on 5xx
			hc.loadBalancer.MarkUnhealthy(baseURL)
		} else {
			hc.loadBalancer.MarkUnhealthy(baseURL)

			// Check if this is a timeout error (expected for long polling)
			if isTimeoutError(err) {
				logDebug("HttpClient.doRequest", map[string]interface{}{
					"status":  "timeout",
					"attempt": attempt,
				})
				// Don't retry on timeout for long polling
				if timeoutMs > 0 {
					return nil, err
				}
			}
		}

		// Retry with exponential backoff
		if attempt < maxRetries {
			delay := hc.getRetryDelay(attempt)
			logWarn("HttpClient.doRequest", map[string]interface{}{
				"status":     "retry",
				"attempt":    attempt,
				"error":      err.Error(),
				"retryDelay": delay.Milliseconds(),
			})
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
	}

	return nil, fmt.Errorf("request failed after %d attempts: %w", maxRetries+1, lastErr)
}

// resolvedRetry429Policy is the effective (defaults-applied) 429 backoff
// policy for one request. MaxAttempts == 0 means unbounded.
type resolvedRetry429Policy struct {
	MaxAttempts int
	BaseMs      int
	CapMs       int
}

// retry429PolicyFor resolves the effective 429 policy for a request kind.
// See Retry429Config for the default rules.
func (hc *HttpClient) retry429PolicyFor(retryKind string) resolvedRetry429Policy {
	cfg := hc.config.Retry429

	baseMs := 500
	capMs := 30000
	if cfg != nil && cfg.BaseMs > 0 {
		baseMs = cfg.BaseMs
	}
	if cfg != nil && cfg.CapMs > 0 {
		capMs = cfg.CapMs
	}

	maxAttempts := 10 // bounded default for push-like/ordinary requests
	if retryKind == "pop" {
		maxAttempts = 0 // unbounded default for long-poll pop
	}
	if cfg != nil && cfg.MaxAttempts > 0 {
		maxAttempts = cfg.MaxAttempts // explicit override applies to both kinds
	}

	return resolvedRetry429Policy{MaxAttempts: maxAttempts, BaseMs: baseMs, CapMs: capMs}
}

// computeRetry429Delay returns the delay before the next 429 retry attempt.
// Honors Retry-After (seconds) when present, with +-20% jitter to avoid a
// synchronized thundering herd; otherwise falls back to exponential backoff
// (baseMs * 2^attemptIndex, capped at capMs), also jittered +-20%.
func computeRetry429Delay(attemptIndex int, retryAfterSeconds *float64, baseMs, capMs int) time.Duration {
	var delayMs float64
	if retryAfterSeconds != nil && *retryAfterSeconds >= 0 {
		delayMs = *retryAfterSeconds * 1000
	} else {
		delayMs = float64(baseMs)
		for i := 0; i < attemptIndex; i++ {
			delayMs *= 2
			if delayMs >= float64(capMs) {
				break
			}
		}
		if delayMs > float64(capMs) {
			delayMs = float64(capMs)
		}
	}

	jitter := 1 + (rand.Float64()*0.4 - 0.2) // +-20%
	final := delayMs * jitter
	if final < 0 {
		final = 0
	}
	return time.Duration(final * float64(time.Millisecond))
}

// doRequestWithRetry429 runs one logical request against a single URL,
// transparently retrying HTTP 429 responses with backoff until the policy
// for `ro.retryKind` is exhausted (or never, for an unbounded pop policy).
// Any other outcome (success, network/timeout error, non-429 4xx, 5xx) is
// returned immediately -- 429 is the only status this layer treats as
// retryable; 5xx/network retry and cross-backend failover are the caller's
// job (doRequest).
func (hc *HttpClient) doRequestWithRetry429(ctx context.Context, method, url string, body interface{}, timeout int, ro requestOptions) (map[string]interface{}, error) {
	policy := hc.retry429PolicyFor(ro.retryKind)
	tries := 0
	for {
		tries++
		result, err := hc.attemptOnce(ctx, method, url, body, timeout)
		if err == nil {
			return result, nil
		}

		httpErr, ok := err.(*HTTPError)
		if !ok || httpErr.StatusCode != 429 {
			return nil, err
		}

		if policy.MaxAttempts > 0 && tries >= policy.MaxAttempts {
			logError("HttpClient.retry429", map[string]interface{}{
				"url":      url,
				"method":   method,
				"error":    "max 429 attempts exhausted",
				"attempts": tries,
				"code":     httpErr.Code,
			})
			return nil, httpErr
		}

		delay := computeRetry429Delay(tries-1, httpErr.RetryAfterSeconds, policy.BaseMs, policy.CapMs)
		logWarn("HttpClient.retry429", map[string]interface{}{
			"url":        url,
			"method":     method,
			"attempt":    tries,
			"retryKind":  ro.retryKind,
			"retryDelay": delay.Milliseconds(),
			"code":       httpErr.Code,
		})
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
			continue
		}
	}
}

// attemptOnce performs exactly one HTTP round trip and parses the result.
// Non-2xx responses come back as *HTTPError (StatusCode/Body/Code, plus
// RetryAfterSeconds on a 429); anything else (marshal/transport/timeout
// errors) comes back as a plain error.
func (hc *HttpClient) attemptOnce(ctx context.Context, method, url string, body interface{}, timeout int) (map[string]interface{}, error) {
	// Create request body
	var bodyReader io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(jsonBody)
	}

	// Create request with context
	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Millisecond)
	req, err := http.NewRequestWithContext(reqCtx, method, url, bodyReader)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if hc.config.BearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+hc.config.BearerToken)
	}
	for key, value := range hc.config.Headers {
		req.Header.Set(key, value)
		// net/http writes the Host line from req.Host (falling back to the URL),
		// ignoring any "Host" entry in req.Header. A queen_proxy deployment
		// routes to a cluster by Host, so a configured Host header has to land
		// on the field or the request reaches the wrong cluster (or none).
		if http.CanonicalHeaderKey(key) == "Host" {
			req.Host = value
		}
	}

	// Execute request.
	//
	// IMPORTANT: do NOT cancel() here. http.Client.Do returns once the
	// response headers are received; the body keeps streaming on the
	// same request context. Cancelling now races against io.ReadAll
	// below and surfaces as "context canceled" for any response that
	// isn't fully buffered (chunked transfer-encoding, large bodies).
	// We cancel after the body has been read+closed.
	resp, err := hc.client.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}

	// Read response body. Body must be fully read before cancel() so the
	// per-attempt timeout context doesn't cut the stream short.
	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	cancel()

	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Check status code
	if resp.StatusCode >= 400 {
		httpErr := &HTTPError{
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
			Code:       extractErrorCode(respBody),
		}
		if resp.StatusCode == 429 {
			httpErr.RetryAfterSeconds = parseRetryAfterHeader(resp.Header.Get("Retry-After"))
		}
		return nil, httpErr
	}

	// Parse response
	var result map[string]interface{}
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &result); err != nil {
			// Try to parse as array
			var arrayResult []interface{}
			if err2 := json.Unmarshal(respBody, &arrayResult); err2 == nil {
				return map[string]interface{}{"data": arrayResult}, nil
			}
			// Return raw body as string
			return map[string]interface{}{"raw": string(respBody)}, nil
		}
	}

	logDebug("HttpClient.doRequest", map[string]interface{}{
		"status": "success",
		"code":   resp.StatusCode,
	})

	return result, nil
}

// extractErrorCode pulls the proxy error contract's machine-readable "code"
// field out of a JSON error body (e.g. "rate_limited", "quota_exceeded",
// "cluster_suspended", "storage_quota_exceeded", "feature_gated",
// "forbidden"). Returns "" when absent or the body isn't a JSON object.
func extractErrorCode(respBody []byte) string {
	if len(respBody) == 0 {
		return ""
	}
	var parsed struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return ""
	}
	return parsed.Code
}

// parseRetryAfterHeader parses the Retry-After header value (seconds, per
// the proxy contract) into a *float64, or nil when absent/non-numeric.
func parseRetryAfterHeader(value string) *float64 {
	if value == "" {
		return nil
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || seconds < 0 {
		return nil
	}
	return &seconds
}

// getRetryDelay calculates the retry delay with exponential backoff.
func (hc *HttpClient) getRetryDelay(attempt int) time.Duration {
	delay := hc.config.RetryDelayMillis
	for i := 0; i < attempt; i++ {
		delay *= 2
	}
	return time.Duration(delay) * time.Millisecond
}

// isTimeoutError checks if an error is a timeout error.
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "timed out") ||
		strings.Contains(errStr, "deadline exceeded")
}

// isNetworkError checks if an error is a network error.
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "no such host") ||
		strings.Contains(errStr, "network is unreachable") ||
		strings.Contains(errStr, "connection reset")
}

// HTTPError represents an HTTP error response.
type HTTPError struct {
	StatusCode int
	Body       string
	// Code is the machine-readable error code from the JSON body's "code"
	// field, when present. Proxy contract: 429 -> "rate_limited" |
	// "quota_exceeded"; 403 -> "cluster_suspended" | "storage_quota_exceeded"
	// | "feature_gated" | "forbidden". Empty when the body has no such field
	// (e.g. errors from a broker that predates the proxy contract).
	Code string
	// RetryAfterSeconds is parsed from the Retry-After response header on a
	// 429 (nil when absent or non-numeric).
	RetryAfterSeconds *float64
}

func (e *HTTPError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("HTTP %d [%s]: %s", e.StatusCode, e.Code, e.Body)
	}
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body)
}

// IsClusterSuspended reports whether this is the terminal cluster_suspended
// 403: callers such as consumer loops should stop entirely rather than
// backing off and retrying (nothing short of operator intervention resolves
// it). Other 403 codes (storage_quota_exceeded, feature_gated, forbidden)
// are still non-retryable but are surfaced instead of specially named here.
func (e *HTTPError) IsClusterSuspended() bool {
	return e.StatusCode == 403 && e.Code == "cluster_suspended"
}

// GetLoadBalancer returns the load balancer (for testing).
func (hc *HttpClient) GetLoadBalancer() *LoadBalancer {
	return hc.loadBalancer
}

// Close closes the HTTP client.
func (hc *HttpClient) Close() {
	// Release pooled keep-alive connections, then reset load balancer state.
	if hc.client != nil {
		if tr, ok := hc.client.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
	hc.loadBalancer.Reset()
}
