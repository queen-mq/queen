package util

import (
	"math"
	"math/rand"
	"time"
)

// Backoff implements exponential backoff with jitter, mirroring the JS
// makeBackoff helper.
type Backoff struct {
	InitialMs  float64
	MaxMs      float64
	Multiplier float64
	attempt    int
}

// NewBackoff returns a Backoff with sensible defaults (initial=50ms,
// max=5000ms, multiplier=2).
func NewBackoff(initialMs, maxMs, multiplier float64) *Backoff {
	if initialMs == 0 {
		initialMs = 50
	}
	if maxMs == 0 {
		maxMs = 5000
	}
	if multiplier == 0 {
		multiplier = 2
	}
	return &Backoff{InitialMs: initialMs, MaxMs: maxMs, Multiplier: multiplier}
}

// Next returns the next backoff duration in milliseconds (with up to 30%
// jitter). Increments the internal attempt counter.
func (b *Backoff) Next() time.Duration {
	ms := math.Min(b.MaxMs, math.Floor(b.InitialMs*math.Pow(b.Multiplier, float64(b.attempt))))
	b.attempt++
	jitter := rand.Float64() * ms * 0.3
	return time.Duration(ms+jitter) * time.Millisecond
}

// Reset zeroes the attempt counter.
func (b *Backoff) Reset() {
	b.attempt = 0
}
