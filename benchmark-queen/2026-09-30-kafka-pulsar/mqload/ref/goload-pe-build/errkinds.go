package main

// errkinds.go — error classification for the cloud mode.
//
// The campaign has to distinguish "the cell is saturated" (5xx / timeouts)
// from "the PROXY refused on purpose" (429 rate_limited / quota_exceeded, 403
// storage_quota_exceeded / cluster_suspended / feature_gated). Those are
// completely different findings and a single `errs=N` counter hides the
// difference, so every failed call is classified into a coarse KIND and, when
// the server sent one, the machine-readable CODE from the JSON body, plus the
// Retry-After the proxy asked for.
//
// client-go wraps errors (`push request failed: %w` -> `request failed after N
// attempts: %w`), so classification must use errors.As/Is, never a type
// assertion on the outer error.

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"

	queen "github.com/smartpricing/queen/clients/client-go"
)

const (
	errKind429     = "http_429" // proxy rate limit / quota
	errKind403     = "http_403" // proxy refusal (suspended, storage quota, feature gate)
	errKind4xx     = "http_4xx" // any other client error (400/404/421/...)
	errKind5xx     = "http_5xx" // broker or proxy failure
	errKindTimeout = "timeout"  // client-side deadline / net timeout
	errKindConn    = "conn"     // refused / reset / EOF / unreachable
	errKindCancel  = "canceled" // run shutting down — never a failure
	errKindOther   = "other"
)

// errSample is one classified failure.
type errSample struct {
	Kind          string
	Code          string // machine-readable "code" field from the body ("" when absent)
	Status        int    // HTTP status, 0 for transport-level failures
	RetryAfterSec float64
	HasRetryAfter bool
}

// classifyErr buckets a client-go error. Order matters: a *queen.HTTPError is
// checked before the transport heuristics because an HTTP-level refusal is
// never a network problem.
func classifyErr(err error) errSample {
	if err == nil {
		return errSample{Kind: ""}
	}
	if errors.Is(err, context.Canceled) {
		return errSample{Kind: errKindCancel}
	}

	var he *queen.HTTPError
	if errors.As(err, &he) {
		s := errSample{Code: he.Code, Status: he.StatusCode}
		if he.RetryAfterSeconds != nil {
			s.RetryAfterSec = *he.RetryAfterSeconds
			s.HasRetryAfter = true
		}
		switch {
		case he.StatusCode == 429:
			s.Kind = errKind429
		case he.StatusCode == 403:
			s.Kind = errKind403
		case he.StatusCode >= 500:
			s.Kind = errKind5xx
		case he.StatusCode >= 400:
			s.Kind = errKind4xx
		default:
			s.Kind = errKindOther
		}
		return s
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return errSample{Kind: errKindTimeout}
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return errSample{Kind: errKindTimeout}
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded"):
		return errSample{Kind: errKindTimeout}
	case strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "eof"),
		strings.Contains(msg, "no such host"),
		strings.Contains(msg, "network is unreachable"),
		strings.Contains(msg, "cannot assign requested address"),
		strings.Contains(msg, "too many open files"):
		return errSample{Kind: errKindConn}
	}
	return errSample{Kind: errKindOther}
}

// errCounts is the serializable snapshot of one operation family's failures.
type errCounts struct {
	Total    int64            `json:"total"`
	ByKind   map[string]int64 `json:"byKind"`
	ByCode   map[string]int64 `json:"byCode"` // "429/rate_limited", "403/storage_quota_exceeded"
	RetryN   int64            `json:"retryAfterSamples"`
	RetryAvg float64          `json:"retryAfterAvgSec"`
	RetryMax float64          `json:"retryAfterMaxSec"`
	// FirstMsg keeps one example message per kind — enough to explain a
	// surprising 4xx without dumping every failure.
	FirstMsg map[string]string `json:"firstMessage"`
}

// errStats accumulates classified failures for one operation family
// (push / pop / ack / configure). A single mutex is enough: even a full 429
// storm is a few 10k lock acquisitions per second.
type errStats struct {
	mu       sync.Mutex
	c        errCounts
	retrySum float64
}

func newErrStats() *errStats {
	return &errStats{c: errCounts{
		ByKind:   map[string]int64{},
		ByCode:   map[string]int64{},
		FirstMsg: map[string]string{},
	}}
}

// record classifies err and folds it in. Returns the sample so callers can
// react (e.g. stop on a terminal 403). Cancellations are recorded under
// "canceled" but do NOT count toward Total — shutting down is not a failure.
func (s *errStats) record(err error) errSample {
	sm := classifyErr(err)
	if sm.Kind == "" {
		return sm
	}
	s.mu.Lock()
	s.c.ByKind[sm.Kind]++
	if sm.Kind != errKindCancel {
		s.c.Total++
	}
	if sm.Status > 0 {
		key := itoa(sm.Status)
		if sm.Code != "" {
			key += "/" + sm.Code
		}
		s.c.ByCode[key]++
	}
	if sm.HasRetryAfter {
		s.c.RetryN++
		s.retrySum += sm.RetryAfterSec
		if sm.RetryAfterSec > s.c.RetryMax {
			s.c.RetryMax = sm.RetryAfterSec
		}
	}
	if _, seen := s.c.FirstMsg[sm.Kind]; !seen && sm.Kind != errKindCancel {
		m := err.Error()
		if len(m) > 240 {
			m = m[:240]
		}
		s.c.FirstMsg[sm.Kind] = m
	}
	s.mu.Unlock()
	return sm
}

// snapshot returns a deep copy safe to read/marshal while the run continues.
func (s *errStats) snapshot() errCounts {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := errCounts{
		Total:    s.c.Total,
		ByKind:   map[string]int64{},
		ByCode:   map[string]int64{},
		RetryN:   s.c.RetryN,
		RetryMax: s.c.RetryMax,
		FirstMsg: map[string]string{},
	}
	for k, v := range s.c.ByKind {
		out.ByKind[k] = v
	}
	for k, v := range s.c.ByCode {
		out.ByCode[k] = v
	}
	for k, v := range s.c.FirstMsg {
		out.FirstMsg[k] = v
	}
	if s.c.RetryN > 0 {
		out.RetryAvg = s.retrySum / float64(s.c.RetryN)
	}
	return out
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [8]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
