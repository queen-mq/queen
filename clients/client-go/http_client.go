package queen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

	// Create HTTP client with timeout
	httpClient := &http.Client{
		Timeout: time.Duration(config.TimeoutMillis) * time.Millisecond,
	}

	return &HttpClient{
		loadBalancer: lb,
		config:       config,
		client:       httpClient,
	}, nil
}

// Get performs a GET request with retry logic.
func (hc *HttpClient) Get(ctx context.Context, path string, timeoutMs int, affinityKey string) (map[string]interface{}, error) {
	return hc.doRequest(ctx, http.MethodGet, path, nil, timeoutMs, affinityKey)
}

// Post performs a POST request with retry logic.
func (hc *HttpClient) Post(ctx context.Context, path string, body interface{}) (map[string]interface{}, error) {
	return hc.doRequest(ctx, http.MethodPost, path, body, 0, "")
}

// PostWithAffinity performs a POST request with affinity key.
func (hc *HttpClient) PostWithAffinity(ctx context.Context, path string, body interface{}, affinityKey string) (map[string]interface{}, error) {
	return hc.doRequest(ctx, http.MethodPost, path, body, 0, affinityKey)
}

// Delete performs a DELETE request with retry logic.
func (hc *HttpClient) Delete(ctx context.Context, path string) (map[string]interface{}, error) {
	return hc.doRequest(ctx, http.MethodDelete, path, nil, 0, "")
}

// doRequest performs an HTTP request with retry logic.
func (hc *HttpClient) doRequest(ctx context.Context, method, path string, body interface{}, timeoutMs int, affinityKey string) (map[string]interface{}, error) {
	var lastErr error

	// Use custom timeout if provided
	timeout := hc.config.TimeoutMillis
	if timeoutMs > 0 {
		timeout = timeoutMs
	}

	for attempt := 0; attempt <= hc.config.RetryAttempts; attempt++ {
		// Get URL from load balancer
		baseURL := hc.loadBalancer.GetURL(affinityKey)
		if baseURL == "" {
			return nil, fmt.Errorf("no available servers")
		}

		url := baseURL + path

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
		}

		logDebug("HttpClient.doRequest", map[string]interface{}{
			"method":  method,
			"url":     url,
			"attempt": attempt,
		})

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
			lastErr = err
			hc.loadBalancer.MarkUnhealthy(baseURL)

			// Check if context was cancelled
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

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

			// Retry with exponential backoff
			if attempt < hc.config.RetryAttempts {
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
			continue
		}

		// Read response body. Body must be fully read before cancel() so the
		// per-attempt timeout context doesn't cut the stream short.
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()

		if err != nil {
			lastErr = fmt.Errorf("failed to read response body: %w", err)
			continue
		}

		// Check status code
		if resp.StatusCode >= 400 {
			lastErr = &HTTPError{
				StatusCode: resp.StatusCode,
				Body:       string(respBody),
			}

			// Don't retry on 4xx errors
			if resp.StatusCode >= 400 && resp.StatusCode < 500 {
				logError("HttpClient.doRequest", map[string]interface{}{
					"status":     resp.StatusCode,
					"body":       string(respBody),
					"noRetry":    true,
				})
				return nil, lastErr
			}

			// Mark unhealthy on 5xx
			if resp.StatusCode >= 500 {
				hc.loadBalancer.MarkUnhealthy(baseURL)
			}

			// Retry with exponential backoff
			if attempt < hc.config.RetryAttempts {
				delay := hc.getRetryDelay(attempt)
				logWarn("HttpClient.doRequest", map[string]interface{}{
					"status":     resp.StatusCode,
					"attempt":    attempt,
					"retryDelay": delay.Milliseconds(),
				})
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(delay):
					continue
				}
			}
			continue
		}

		// Mark healthy on success
		hc.loadBalancer.MarkHealthy(baseURL)

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
			"status":  "success",
			"code":    resp.StatusCode,
		})

		return result, nil
	}

	return nil, fmt.Errorf("request failed after %d attempts: %w", hc.config.RetryAttempts+1, lastErr)
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
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body)
}

// GetLoadBalancer returns the load balancer (for testing).
func (hc *HttpClient) GetLoadBalancer() *LoadBalancer {
	return hc.loadBalancer
}

// Close closes the HTTP client.
func (hc *HttpClient) Close() {
	// HTTP client doesn't need explicit closing in Go
	// but we reset the load balancer state
	hc.loadBalancer.Reset()
}
