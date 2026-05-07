// Package timefmt parses the time strings accepted by queenctl flags.
//
// Supported forms:
//   - RFC3339 / RFC3339Nano (e.g. 2026-05-06T12:34:56Z)
//   - YYYY-MM-DD (interpreted as UTC midnight)
//   - Unix seconds: 1714998000
//   - Relative: "5m ago", "2h ago", "1d ago", "now"
package timefmt

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Parse converts s into a time.Time. Returns an error if no format matches.
func Parse(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty time")
	}
	low := strings.ToLower(s)
	if low == "now" {
		return time.Now(), nil
	}
	if low == "beginning" || low == "start" || low == "earliest" {
		return time.Time{}, nil
	}

	// Relative: "<N><unit> ago"
	if strings.HasSuffix(low, " ago") {
		body := strings.TrimSpace(strings.TrimSuffix(low, " ago"))
		d, err := parseDuration(body)
		if err == nil {
			return time.Now().Add(-d), nil
		}
	}
	// Bare relative: "5m", "2h", "30s"
	if d, err := parseDuration(low); err == nil {
		return time.Now().Add(-d), nil
	}

	// Unix seconds (integer only, large enough to not collide with year).
	if n, err := strconv.ParseInt(s, 10, 64); err == nil && n > 1_000_000_000 {
		return time.Unix(n, 0), nil
	}

	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised time %q", s)
}

// MustParse panics on error - intended for tests / fixtures only.
func MustParse(s string) time.Time {
	t, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return t
}

// FormatRFC3339 renders t for the API. Returns empty string for zero time.
func FormatRFC3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	// Go stdlib accepts "5m", "2h" but not "1d".
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	if strings.HasSuffix(s, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err == nil {
			return time.Duration(n) * 24 * time.Hour, nil
		}
	}
	if strings.HasSuffix(s, "w") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "w"))
		if err == nil {
			return time.Duration(n) * 7 * 24 * time.Hour, nil
		}
	}
	return 0, fmt.Errorf("unrecognised duration %q", s)
}
