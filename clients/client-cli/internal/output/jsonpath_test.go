package output

import "testing"

func TestEvalJSONPath(t *testing.T) {
	data := map[string]any{
		"queue": map[string]any{
			"name":  "orders",
			"depth": float64(42),
		},
		"groups": []any{
			map[string]any{"name": "analyzer"},
			map[string]any{"name": "tracer"},
		},
	}
	cases := []struct {
		expr string
		want any
	}{
		{".queue.name", "orders"},
		{"queue.depth", float64(42)},
		{".groups[0].name", "analyzer"},
		{".groups[1].name", "tracer"},
	}
	for _, tc := range cases {
		got, err := evalJSONPath(data, tc.expr)
		if err != nil {
			t.Errorf("%s: %v", tc.expr, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.expr, got, tc.want)
		}
	}
}

func TestEvalJSONPathErrors(t *testing.T) {
	// Empty expr is identity, not an error - the wrapper checks for that.
	if _, err := evalJSONPath(map[string]any{"a": "b"}, ".missing.deep"); err == nil {
		t.Error("expected error on missing key chain (non-object)")
	}
	if _, err := evalJSONPath(map[string]any{"arr": []any{1, 2}}, ".arr[5]"); err == nil {
		t.Error("expected out-of-range error")
	}
	if _, err := evalJSONPath(map[string]any{"arr": []any{1, 2}}, ".arr["); err == nil {
		t.Error("expected unterminated bracket error")
	}
}

func TestParseFormat(t *testing.T) {
	cases := []struct {
		in   string
		f    Format
		expr string
	}{
		{"", "", ""},
		{"json", FormatJSON, ""},
		{"ndjson", FormatNDJSON, ""},
		{"jsonl", FormatNDJSON, ""},
		{"YAML", FormatYAML, ""},
		{"jsonpath=.foo", FormatJSONPath, ".foo"},
	}
	for _, tc := range cases {
		f, expr, err := ParseFormat(tc.in)
		if err != nil {
			t.Errorf("%s: %v", tc.in, err)
			continue
		}
		if f != tc.f || expr != tc.expr {
			t.Errorf("%s: got (%q,%q), want (%q,%q)", tc.in, f, expr, tc.f, tc.expr)
		}
	}
	if _, _, err := ParseFormat("garbage"); err == nil {
		t.Error("expected parse error")
	}
}
