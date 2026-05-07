package output

import (
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

// AsArray normalises a map[string]any field into a []any. Returns nil if
// the field is absent or not array-shaped.
func AsArray(in any, key string) []any {
	m, ok := in.(map[string]any)
	if !ok {
		return nil
	}
	v, ok := m[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	return arr
}

// AsMap returns in as a map[string]any when possible.
func AsMap(in any) map[string]any {
	if m, ok := in.(map[string]any); ok {
		return m
	}
	return nil
}

// FirstNonNil returns the first non-nil/empty value resolved from the keys
// in the order given. Useful for tolerating server response variation.
func FirstNonNil(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil && v != "" {
			return v
		}
	}
	return nil
}

// HumanDuration renders a numeric seconds (or string) as a humanised
// duration: "30s", "12m", "1h4m". Used by the table formatter for lag.
func HumanDuration(v any) string {
	switch x := v.(type) {
	case nil:
		return "-"
	case float64:
		return durSeconds(x)
	case int:
		return durSeconds(float64(x))
	case int64:
		return durSeconds(float64(x))
	case string:
		if x == "" {
			return "-"
		}
		if f, err := strconv.ParseFloat(x, 64); err == nil {
			return durSeconds(f)
		}
		return x
	default:
		return defaultFormat(v)
	}
}

func durSeconds(s float64) string {
	if s == 0 {
		return "0s"
	}
	d := time.Duration(s * float64(time.Second))
	// time.Duration.String() uses microsecond precision; smooth to a nicer
	// shape for the table view.
	switch {
	case d < time.Second:
		return d.Round(time.Millisecond).String()
	case d < time.Minute:
		return d.Round(time.Second).String()
	case d < time.Hour:
		return d.Round(time.Second).String()
	default:
		return d.Round(time.Minute).String()
	}
}

// HumanInt returns a comma-grouped integer ("1,234") for the table view.
func HumanInt(v any) string {
	switch x := v.(type) {
	case nil:
		return "-"
	case float64:
		return humanize.Comma(int64(x))
	case int:
		return humanize.Comma(int64(x))
	case int64:
		return humanize.Comma(x)
	default:
		return defaultFormat(v)
	}
}

// HumanRate appends "/s" to a count, useful for messages-per-second columns.
func HumanRate(v any) string {
	s := defaultFormat(v)
	if s == "-" {
		return s
	}
	if strings.HasSuffix(s, "/s") {
		return s
	}
	return s + "/s"
}
