// Package sdk wires the resolved CLI context to a *queen.Queen client. It
// is a thin adapter so individual commands don't repeat the boilerplate.
package sdk

import (
	"context"
	"fmt"

	"github.com/smartpricing/queen/client-cli/internal/config"
	queen "github.com/smartpricing/queen/clients/client-go"
)

// Client is a typed handle bundling the *queen.Queen plus its Admin facet.
type Client struct {
	Q       *queen.Queen
	A       *queen.Admin
	Server  string
	Context string
}

// New builds a client from a resolved CLI context.
func New(r *config.Resolved) (*Client, error) {
	if r == nil {
		return nil, fmt.Errorf("nil resolved context")
	}
	q, err := queen.New(queen.ClientConfig{
		URL:           r.Server,
		BearerToken:   r.Token,
		TimeoutMillis: 30_000,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", r.Server, err)
	}
	return &Client{
		Q:       q,
		A:       q.Admin(),
		Server:  r.Server,
		Context: r.Name,
	}, nil
}

// Close flushes buffers and closes the underlying client.
func (c *Client) Close(ctx context.Context) error {
	if c == nil || c.Q == nil {
		return nil
	}
	return c.Q.Close(ctx)
}
