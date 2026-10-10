package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// client is the two HTTP surfaces the provider talks to: the broker's API
// (queues) and, when it is configured, the proxy's control plane (a
// cluster's S3 sink).
type client struct {
	http *http.Client
	// The broker, or the proxy in front of it: "http://queen:6632".
	endpoint string
	// Sent as "Authorization: Bearer": a proxy API key or a JWT. Empty for a
	// broker port with no authentication.
	token string
	// The proxy: "http://queen-proxy:6711". Empty when no control-plane
	// resource is used.
	cpEndpoint string
	cpToken    string
}

// apiError is an answer that is not a success: the status and what the
// broker said, which names the field or the rule.
type apiError struct {
	Status int
	Code   string
	Body   string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("the broker answered %d: %s", e.Status, e.Body)
}

func (c *client) do(ctx context.Context, base, method, path string, header map[string]string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		var e struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(raw, &e)
		return &apiError{Status: res.StatusCode, Code: e.Code, Body: strings.TrimSpace(string(raw))}
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("%s %s answered %d with a body that is not the JSON expected: %w", method, path, res.StatusCode, err)
		}
	}
	return nil
}

func (c *client) broker(ctx context.Context, method, path string, in, out any) error {
	h := map[string]string{}
	if c.token != "" {
		h["Authorization"] = "Bearer " + c.token
	}
	return c.do(ctx, c.endpoint, method, path, h, in, out)
}

func (c *client) controlPlane(ctx context.Context, method, path string, in, out any) error {
	if c.cpEndpoint == "" || c.cpToken == "" {
		return fmt.Errorf("this resource is set through the proxy's control plane: give the provider cp_endpoint and cp_token (or QUEEN_CP_ENDPOINT and QUEEN_CP_TOKEN)")
	}
	return c.do(ctx, c.cpEndpoint, method, path, map[string]string{"x-queen-cp-token": c.cpToken}, in, out)
}

func notFound(err error) bool {
	e, ok := err.(*apiError)
	return ok && e.Status == http.StatusNotFound
}

func newClient(endpoint, token, cpEndpoint, cpToken string) *client {
	return &client{
		http:       &http.Client{Timeout: 60 * time.Second},
		endpoint:   endpoint,
		token:      token,
		cpEndpoint: cpEndpoint,
		cpToken:    cpToken,
	}
}

// ---------------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------------

// queueOptions is the part of a queue's options the provider manages. A nil
// field is left out of the request.
type queueOptions struct {
	LeaseTime                 *int64 `json:"leaseTime,omitempty"`
	RetryLimit                *int64 `json:"retryLimit,omitempty"`
	DeadLetterQueue           *bool  `json:"deadLetterQueue,omitempty"`
	DlqAfterMaxRetries        *bool  `json:"dlqAfterMaxRetries,omitempty"`
	DedupWindowSeconds        *int64 `json:"dedupWindowSeconds,omitempty"`
	DelayedProcessing         *int64 `json:"delayedProcessing,omitempty"`
	WindowBuffer              *int64 `json:"windowBuffer,omitempty"`
	RetentionEnabled          *bool  `json:"retentionEnabled,omitempty"`
	RetentionSeconds          *int64 `json:"retentionSeconds,omitempty"`
	CompletedRetentionSeconds *int64 `json:"completedRetentionSeconds,omitempty"`
	MaxWaitTimeSeconds        *int64 `json:"maxWaitTimeSeconds,omitempty"`
	EncryptionEnabled         *bool  `json:"encryptionEnabled,omitempty"`
}

type queue struct {
	ID        string       `json:"id"`
	Name      string       `json:"name"`
	Namespace string       `json:"namespace"`
	Task      string       `json:"task"`
	Options   queueOptions `json:"options"`
}

// configureQueue creates the queue or sets its options. The broker merges:
// an option, or a label, the request leaves out keeps its stored value, so a
// queue that already exists is taken over without touching what the
// configuration does not name.
func (c *client) configureQueue(ctx context.Context, name string, namespace, task *string, o queueOptions) error {
	body := map[string]any{"queue": name, "options": o}
	if namespace != nil {
		body["namespace"] = *namespace
	}
	if task != nil {
		body["task"] = *task
	}
	return c.broker(ctx, http.MethodPost, "/api/v1/configure", body, nil)
}

func (c *client) getQueue(ctx context.Context, name string) (*queue, error) {
	var q queue
	if err := c.broker(ctx, http.MethodGet, "/api/v1/resources/queues/"+url.PathEscape(name), nil, &q); err != nil {
		return nil, err
	}
	return &q, nil
}

// deleteQueue deletes the queue and every message in it.
func (c *client) deleteQueue(ctx context.Context, name string) error {
	return c.broker(ctx, http.MethodDelete, "/api/v1/resources/queues/"+url.PathEscape(name), nil, nil)
}

// ---------------------------------------------------------------------------
// The S3 sink of a cluster
// ---------------------------------------------------------------------------

// s3Sink is the stored sink as the control plane answers it: never its secret.
type s3Sink struct {
	Cluster      string         `json:"cluster"`
	Enabled      bool           `json:"enabled"`
	Config       map[string]any `json:"config"`
	SecretKeySet bool           `json:"secretKeySet"`
	UpdatedAt    string         `json:"updatedAt"`
}

// putS3Sink stores the sink's settings. The secret is sent only when it is
// not empty: left out, the stored one is kept.
func (c *client) putS3Sink(ctx context.Context, cluster string, enabled bool, config map[string]any, secretKey string) (*s3Sink, error) {
	body := map[string]any{"enabled": enabled}
	for k, v := range config {
		body[k] = v
	}
	if secretKey != "" {
		body["secretKey"] = secretKey
	}
	var s s3Sink
	if err := c.controlPlane(ctx, http.MethodPut, "/api/cp/clusters/"+url.PathEscape(cluster)+"/s3", body, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (c *client) getS3Sink(ctx context.Context, cluster string) (*s3Sink, error) {
	var s s3Sink
	if err := c.controlPlane(ctx, http.MethodGet, "/api/cp/clusters/"+url.PathEscape(cluster)+"/s3", nil, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func (c *client) deleteS3Sink(ctx context.Context, cluster string) error {
	return c.controlPlane(ctx, http.MethodDelete, "/api/cp/clusters/"+url.PathEscape(cluster)+"/s3", nil, nil)
}
