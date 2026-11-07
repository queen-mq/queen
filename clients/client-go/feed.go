package queen

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/smartpricing/queen/clients/client-go/v2/feed"
)

// The tenant feed: every push to the tenant's queues, in the order the
// broker's log committed them, read by position (POST /api/v1/feed). See
// package feed.
//
//	// What happened to one booking today, then stop.
//	r, _ := client.Feed(queen.FeedOptions{Partitions: []string{"booking-9137"}, Since: "1d", Follow: feed.Bool(false)})
//	for e, err := range r.Entries(ctx) { ... }
//
// Followers serve it: a leader answers 421 with the followers to ask, and the
// reader then reads from those (and goes back to the configured URL if none
// answers). A single broker serves it itself.
type (
	FeedOptions   = feed.Options
	FeedReader    = feed.Reader
	FeedEntry     = feed.Entry
	FeedPage      = feed.Page
	FeedCondition = feed.Condition
	FeedShard     = feed.Shard
)

// Feed is a reader of the tenant feed (JS queen.feed(options)).
func (q *Queen) Feed(opts FeedOptions) (*FeedReader, error) {
	scheme := "http"
	urls := q.httpClient.loadBalancer.GetAllURLs()
	if len(urls) > 0 && strings.HasPrefix(urls[0], "https://") {
		scheme = "https"
	}
	return feed.NewReader(&feedPoster{hc: q.httpClient}, q.nodePoster, scheme, opts)
}

// nodePoster is a client to one node, with this client's credentials and timeouts.
func (q *Queen) nodePoster(baseURL string) (feed.Poster, error) {
	cfg := q.httpClient.config
	cfg.URL = baseURL
	cfg.URLs = nil
	// A node's own address: the configured Host (a proxy's routing) is not its.
	if len(cfg.Headers) > 0 {
		h := make(map[string]string, len(cfg.Headers))
		for k, v := range cfg.Headers {
			if http.CanonicalHeaderKey(k) != "Host" {
				h[k] = v
			}
		}
		cfg.Headers = h
	}
	hc, err := NewHttpClient(cfg)
	if err != nil {
		return nil, err
	}
	return &feedPoster{hc: hc, own: true}, nil
}

type feedPoster struct {
	hc  *HttpClient
	own bool
}

func (p *feedPoster) PostFeed(ctx context.Context, body []byte, timeout time.Duration) ([]byte, error) {
	raw, err := p.hc.PostRawTimeout(ctx, "/api/v1/feed", json.RawMessage(body), int(timeout/time.Millisecond))
	if err != nil {
		return nil, feedStatusError(err)
	}
	return raw, nil
}

func (p *feedPoster) Close() {
	if p.own {
		p.hc.Close()
	}
}

// feedStatusError reads an HTTP refusal as the feed reader reads one: status, code, parsed body.
func feedStatusError(err error) error {
	var he *HTTPError
	if !errors.As(err, &he) {
		return err
	}
	se := &feed.StatusError{Status: he.StatusCode, Code: he.Code, Err: err}
	var body map[string]interface{}
	if json.Unmarshal([]byte(he.Body), &body) == nil {
		se.Body = body
		if m, ok := body["error"].(string); ok && m != "" {
			se.Message = m
		}
	}
	if se.Message == "" {
		se.Message = he.Error()
	}
	return se
}
