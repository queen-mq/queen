package jsv

import (
	"math"
	"testing"
)

func TestNumbersAsJSWritesThem(t *testing.T) {
	cases := map[float64]string{
		60: "60", 0.5: "0.5", 1e21: "1e+21", 1e20: "100000000000000000000", 1.5e-7: "1.5e-7",
		0.000001: "0.000001", -3: "-3", 123456789.25: "123456789.25", 1793955600000: "1793955600000",
		math.Copysign(0, -1): "0",
	}
	for f, want := range cases {
		if got := NumberString(f); got != want {
			t.Errorf("NumberString(%v) = %q, want %q", f, got, want)
		}
		if got := string(MustMarshal(f)); got != want {
			t.Errorf("Marshal(%v) = %q, want %q", f, got, want)
		}
	}
}

func TestMarshalIsJSONStringify(t *testing.T) {
	v := Ordered{{"kind", "window-tumbling"}, {"seconds", 60.0}, {"x", Undefined}, {"s", "a<b>&\u001f\"\\\n "},
		{"l", []interface{}{1.0, Undefined, nil, true}}}
	want := `{"kind":"window-tumbling","seconds":60,"s":"a<b>&\u001f\"\\\n` + " " + `","l":[1,null,null,true]}`
	if got := string(MustMarshal(v)); got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	m := map[string]interface{}{"b": 2.0, "a": Undefined, "c": map[string]interface{}{"z": nil}}
	if got := string(MustMarshal(m)); got != `{"b":2,"c":{"z":null}}` {
		t.Fatalf("map: %s", got)
	}
}

func TestISOAndDateParse(t *testing.T) {
	if s := MustISO(1793955600000); s != "2026-11-06T09:00:00.000Z" {
		t.Fatal(s)
	}
	if s := MustISO(-1.5); s != "1969-12-31T23:59:59.999Z" {
		t.Fatal(s)
	}
	for in, want := range map[string]float64{
		"2026-11-06T09:00:00.000Z":    1793955600000,
		"2026-11-06T09:00:00.000000Z": 1793955600000,
		"2026-11-06T09:00:00.0009Z":   1793955600000,
		"2026-11-06T10:00:00+01:00":   1793955600000,
		"2026-11-06":                  1793923200000,
		"2026-11-06T09:00Z":           1793955600000,
	} {
		got, ok := DateParse(in)
		if !ok || got != want {
			t.Errorf("DateParse(%q) = %v %v, want %v", in, got, ok, want)
		}
	}
	if _, ok := DateParse("yesterday"); ok {
		t.Error("yesterday parsed")
	}
}

func TestStringAndTruthy(t *testing.T) {
	if String(3.0) != "3" || String(Undefined) != "undefined" || String(nil) != "null" || String([]interface{}{1.0, nil, "a"}) != "1,,a" {
		t.Fatal("String")
	}
	for _, v := range []interface{}{false, 0.0, "", nil, Undefined, math.NaN()} {
		if Truthy(v) {
			t.Errorf("%v is truthy", v)
		}
	}
	for _, v := range []interface{}{true, 1.0, "x", map[string]interface{}{}, []interface{}{}} {
		if !Truthy(v) {
			t.Errorf("%v is falsy", v)
		}
	}
}

func TestSame(t *testing.T) {
	m := map[string]interface{}{"a": 1.0}
	n := map[string]interface{}{"a": 1.0}
	if !Same(m, m) || Same(m, n) || !Same(1.0, 1) || Same("1", 1.0) || !Same(Undefined, Undefined) || Same(nil, Undefined) {
		t.Fatal("Same")
	}
}

func TestParseDurationMs(t *testing.T) {
	for in, want := range map[string]float64{"15m": 900000, "1h30m": 5400000, "250ms": 250, "1.5s": 1500, "7d": 604800000} {
		got, err := ParseDurationMs(in)
		if err != nil || got != want {
			t.Errorf("%s: %v %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "1hour", "0s", "5"} {
		if _, err := ParseDurationMs(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}
