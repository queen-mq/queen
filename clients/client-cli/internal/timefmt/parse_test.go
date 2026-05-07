package timefmt

import (
	"testing"
	"time"
)

func TestParseAbsolute(t *testing.T) {
	cases := []string{
		"2026-05-06T12:34:56Z",
		"2026-05-06T12:34:56.123Z",
		"2026-05-06 12:34:56",
		"2026-05-06",
	}
	for _, s := range cases {
		if _, err := Parse(s); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
}

func TestParseRelative(t *testing.T) {
	now := time.Now()
	for _, s := range []string{"5m ago", "2h ago", "1d ago", "1w ago", "30s"} {
		t0, err := Parse(s)
		if err != nil {
			t.Errorf("%s: %v", s, err)
			continue
		}
		if !t0.Before(now.Add(time.Second)) {
			t.Errorf("%s: not in the past", s)
		}
	}
	if t0, err := Parse("now"); err != nil {
		t.Errorf("now: %v", err)
	} else if t0.IsZero() {
		t.Error("now produced zero time")
	}
	if t0, err := Parse("beginning"); err != nil {
		t.Errorf("beginning: %v", err)
	} else if !t0.IsZero() {
		t.Error("beginning should produce zero time")
	}
}

func TestParseUnix(t *testing.T) {
	got, err := Parse("1714998000")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Unix(1714998000, 0)
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParseInvalid(t *testing.T) {
	for _, s := range []string{"", "tomorrow", "12:34"} {
		if _, err := Parse(s); err == nil {
			t.Errorf("%q: expected error", s)
		}
	}
}

func TestFormatRFC3339(t *testing.T) {
	if FormatRFC3339(time.Time{}) != "" {
		t.Error("zero time should format as empty string")
	}
	t0 := time.Date(2026, 5, 6, 10, 0, 0, 0, time.UTC)
	if FormatRFC3339(t0) != "2026-05-06T10:00:00Z" {
		t.Error("unexpected format")
	}
}
