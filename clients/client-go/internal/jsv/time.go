package jsv

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// maxTime is the largest time value a JS Date holds (8.64e15 ms either side of the epoch).
const maxTime = 8.64e15

// ISO is new Date(ms).toISOString(): `2026-11-06T09:00:00.000Z`. The time
// value is truncated toward zero first, as a Date's is. ok is false where JS
// throws (not finite, out of range).
func ISO(ms float64) (string, bool) {
	if math.IsNaN(ms) || math.IsInf(ms, 0) || math.Abs(ms) > maxTime {
		return "", false
	}
	t := math.Trunc(ms)
	if t == 0 {
		t = 0 // -0
	}
	whole := int64(t)
	sec := whole / 1000
	milli := whole % 1000
	if milli < 0 {
		milli += 1000
		sec--
	}
	tm := time.Unix(sec, milli*int64(time.Millisecond)).UTC()
	y := tm.Year()
	var year string
	switch {
	case y >= 0 && y <= 9999:
		year = fmt.Sprintf("%04d", y)
	case y < 0:
		year = fmt.Sprintf("-%06d", -y)
	default:
		year = fmt.Sprintf("+%06d", y)
	}
	return fmt.Sprintf("%s-%02d-%02dT%02d:%02d:%02d.%03dZ", year, int(tm.Month()), tm.Day(), tm.Hour(), tm.Minute(), tm.Second(), milli), true
}

// MustISO is ISO for a time known to be valid; an invalid one gives "".
func MustISO(ms float64) string {
	s, _ := ISO(ms)
	return s
}

// ISOOf formats a time.Time as toISOString does.
func ISOOf(t time.Time) string {
	return MustISO(float64(t.UnixMilli()))
}

var isoRe = regexp.MustCompile(`^([+-]\d{6}|\d{4})(?:-(\d{2})(?:-(\d{2}))?)?(?:[Tt ](\d{2}):(\d{2})(?::(\d{2})(?:[.,](\d+))?)?\s*(Z|z|[+-]\d{2}(?::?\d{2})?)?)?$`)

// DateParse is Date.parse(s): epoch milliseconds, ok false where JS answers
// NaN. The ISO forms (a date alone is UTC, a date and time without an offset
// is local time, digits past milliseconds are dropped) and the RFC 1123 /
// RFC 850 / ANSI C forms V8 also reads.
func DateParse(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	if m := isoRe.FindStringSubmatch(s); m != nil {
		year, _ := strconv.Atoi(m[1])
		if m[1] == "-000000" {
			return 0, false
		}
		month, day := 1, 1
		if m[2] != "" {
			month, _ = strconv.Atoi(m[2])
		}
		if m[3] != "" {
			day, _ = strconv.Atoi(m[3])
		}
		if month < 1 || month > 12 || day < 1 || day > 31 {
			return 0, false
		}
		hour, minute, second, milli := 0, 0, 0, 0
		hasTime := m[4] != ""
		if hasTime {
			hour, _ = strconv.Atoi(m[4])
			minute, _ = strconv.Atoi(m[5])
			if m[6] != "" {
				second, _ = strconv.Atoi(m[6])
			}
			if m[7] != "" {
				frac := m[7]
				for len(frac) < 3 {
					frac += "0"
				}
				milli, _ = strconv.Atoi(frac[:3])
			}
			if hour > 24 || minute > 59 || second > 59 || (hour == 24 && (minute != 0 || second != 0 || milli != 0)) {
				return 0, false
			}
		}
		loc := time.UTC
		offsetMin := 0
		if z := m[8]; z != "" && z != "Z" && z != "z" {
			sign := 1
			if z[0] == '-' {
				sign = -1
			}
			digits := strings.ReplaceAll(z[1:], ":", "")
			hh, _ := strconv.Atoi(digits[:2])
			mm := 0
			if len(digits) >= 4 {
				mm, _ = strconv.Atoi(digits[2:4])
			}
			offsetMin = sign * (hh*60 + mm)
		} else if z == "" && hasTime {
			// A date and a time with no offset is local time.
			loc = time.Local
		}
		t := time.Date(year, time.Month(month), day, hour, minute, second, milli*int(time.Millisecond), loc)
		ms := float64(t.UnixMilli()) - float64(offsetMin)*60000
		if math.Abs(ms) > maxTime {
			return 0, false
		}
		return ms, true
	}
	for _, layout := range []string{time.RFC1123, time.RFC1123Z, time.RFC850, time.ANSIC, time.UnixDate, time.RubyDate,
		"Mon, 2 Jan 2006 15:04:05 MST", "Mon Jan 2 2006 15:04:05 GMT-0700", "Jan 2 2006 15:04:05", "Jan 2, 2006", "2 Jan 2006"} {
		if t, err := time.Parse(layout, s); err == nil {
			return float64(t.UnixMilli()), true
		}
	}
	return 0, false
}

// TimeOf reads a time the way the feed's eventTime does (extractEventTime in
// _windowCommon.js): a time.Time, epoch milliseconds, or a string Date.parse
// reads. ok false: no time.
func TimeOf(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case nil:
		return 0, false
	case time.Time:
		if x.IsZero() {
			return 0, false
		}
		return float64(x.UnixMilli()), true
	case *time.Time:
		if x == nil || x.IsZero() {
			return 0, false
		}
		return float64(x.UnixMilli()), true
	case string:
		return DateParse(x)
	}
	if IsUndefined(v) {
		return 0, false
	}
	if f, ok := Num(v); ok {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

var durationPartRe = regexp.MustCompile(`\d+(?:\.\d+)?[a-z]+`)
var durationRe = regexp.MustCompile(`^(\d+(?:\.\d+)?)(ms|s|m|h|d)$`)

var durationUnits = map[string]float64{"ms": 1, "s": 1000, "m": 60000, "h": 3600000, "d": 86400000}

// ParseDurationMs is the SDK's duration parser (kv/expiry.js parseDurationMs):
// '250ms', '30s', '15m', '1h30m', '7d', in milliseconds (a fraction stays).
func ParseDurationMs(value string) (float64, error) {
	t := strings.ToLower(strings.TrimSpace(value))
	if t == "" {
		return 0, fmt.Errorf("duration must be a string with a unit, e.g. '250ms', '30s', '15m', '24h', '7d' — got %q", value)
	}
	parts := durationPartRe.FindAllString(t, -1)
	if parts == nil || strings.Join(parts, "") != t {
		return 0, fmt.Errorf("invalid duration '%s': expected <number><unit> parts, e.g. '30s' or '1h30m'", value)
	}
	ms := 0.0
	for _, part := range parts {
		m := durationRe.FindStringSubmatch(part)
		if m == nil {
			return 0, fmt.Errorf("invalid duration '%s': unknown unit in '%s' (use ms, s, m, h, d)", value, part)
		}
		n, _ := strconv.ParseFloat(m[1], 64)
		ms += n * durationUnits[m[2]]
	}
	if !(ms > 0) {
		return 0, fmt.Errorf("invalid duration '%s': must be greater than zero", value)
	}
	return ms, nil
}
