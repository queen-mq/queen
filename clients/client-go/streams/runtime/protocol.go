package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/smartpricing/queen/clients/client-go/streams/operators"
)

// RegisterResult holds /streams/v1/queries response.
type RegisterResult struct {
	QueryID  string
	Name     string
	Fresh    bool
	DidReset bool
}

// RegisterQuery calls /streams/v1/queries (idempotent).
func RegisterQuery(ctx context.Context, c *HTTPClient, name, sourceQueue, sinkQueue, configHash string, reset bool) (*RegisterResult, error) {
	body := map[string]interface{}{
		"name":         name,
		"source_queue": sourceQueue,
		"config_hash":  configHash,
		"reset":        reset,
	}
	if sinkQueue != "" {
		body["sink_queue"] = sinkQueue
	}
	res, err := c.Post(ctx, "/streams/v1/queries", body)
	if err != nil {
		// Surface 409 mismatch as a clear error.
		if he, ok := err.(*HTTPError); ok && he.Status == 409 {
			msg := "config_hash mismatch"
			if e, ok := he.Body["error"].(string); ok {
				msg = e
			}
			return nil, errors.New(msg + "\n\nHint: pass reset=true to wipe state.")
		}
		return nil, err
	}
	if v, ok := res["success"].(bool); ok && !v {
		return nil, errors.New("register failed")
	}
	out := &RegisterResult{}
	if s, ok := res["query_id"].(string); ok {
		out.QueryID = s
	}
	if s, ok := res["name"].(string); ok {
		out.Name = s
	}
	if b, ok := res["fresh"].(bool); ok {
		out.Fresh = b
	}
	if b, ok := res["reset"].(bool); ok {
		out.DidReset = b
	}
	return out, nil
}

// GetState calls /streams/v1/state/get and returns key->value.
func GetState(ctx context.Context, c *HTTPClient, queryID, partitionID string, keys []string) (map[string]interface{}, error) {
	body := map[string]interface{}{
		"query_id":     queryID,
		"partition_id": partitionID,
		"keys":         keys,
	}
	res, err := c.Post(ctx, "/streams/v1/state/get", body)
	if err != nil {
		return nil, err
	}
	out := map[string]interface{}{}
	if rows, ok := res["rows"].([]interface{}); ok {
		for _, r := range rows {
			if row, ok := r.(map[string]interface{}); ok {
				if k, ok := row["key"].(string); ok {
					out[k] = row["value"]
				}
			}
		}
	}
	return out, nil
}

// CommitCycle calls /streams/v1/cycle.
func CommitCycle(ctx context.Context, c *HTTPClient, queryID, partitionID, consumerGroup string,
	stateOps []operators.StateOp, pushItems []operators.PushItem, ack map[string]interface{}, releaseLease bool) (map[string]interface{}, error) {

	// Force empty slices instead of nil so JSON marshals to "[]" and not
	// "null". The streams_cycle_v1 SP's COALESCE only fires for SQL NULL,
	// not JSON null, so a "null" array makes jsonb_array_length() panic
	// with "cannot get array length of a scalar".
	if stateOps == nil {
		stateOps = []operators.StateOp{}
	}
	if pushItems == nil {
		pushItems = []operators.PushItem{}
	}
	body := map[string]interface{}{
		"query_id":       queryID,
		"partition_id":   partitionID,
		"consumer_group": consumerGroup,
		"state_ops":      stateOps,
		"push_items":     pushItems,
		"ack":            ack,
		"release_lease":  releaseLease,
	}
	res, err := c.Post(ctx, "/streams/v1/cycle", body)
	if err != nil {
		return nil, err
	}
	if v, ok := res["success"].(bool); ok && !v {
		// Surface the server's error message + body so the caller can
		// see *why* the cycle failed (state op format mismatch, etc).
		msg := "cycle commit failed"
		if e, ok := res["error"].(string); ok && e != "" {
			msg = e
		}
		return res, fmt.Errorf("%s (body=%v)", msg, res)
	}
	return res, nil
}
