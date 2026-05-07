package output

import (
	"bytes"
	"strings"
	"testing"
)

func sampleData() any {
	return map[string]any{
		"queues": []any{
			map[string]any{
				"name":           "orders",
				"namespace":      "billing",
				"task":           "ingest",
				"partitionCount": float64(8),
				"lagSeconds":     float64(0),
			},
			map[string]any{
				"name":           "events",
				"namespace":      "ops",
				"partitionCount": float64(4),
				"lagSeconds":     float64(125),
			},
		},
	}
}

func sampleView() View {
	return View{
		Columns: []Column{
			{Header: "QUEUE", Path: "name"},
			{Header: "PARTITIONS", Path: "partitionCount", Format: HumanInt},
			{Header: "LAG", Path: "lagSeconds", Format: HumanDuration},
		},
		RowsFrom: func(d any) []any { return AsArray(d, "queues") },
	}
}

func TestRenderTable(t *testing.T) {
	var buf bytes.Buffer
	r := &Renderer{Format: FormatTable, View: sampleView(), Out: &buf}
	if err := r.Render(sampleData()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "QUEUE") || !strings.Contains(out, "orders") || !strings.Contains(out, "2m5s") {
		t.Errorf("unexpected table output:\n%s", out)
	}
}

func TestRenderJSON(t *testing.T) {
	var buf bytes.Buffer
	r := &Renderer{Format: FormatJSON, View: sampleView(), Out: &buf}
	if err := r.Render(sampleData()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `"name": "orders"`) {
		t.Errorf("JSON should include name=orders, got:\n%s", out)
	}
}

func TestRenderNDJSON(t *testing.T) {
	var buf bytes.Buffer
	r := &Renderer{Format: FormatNDJSON, View: sampleView(), Out: &buf}
	if err := r.Render(sampleData()); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d:\n%s", len(lines), buf.String())
	}
	for _, l := range lines {
		if !strings.HasPrefix(l, "{") {
			t.Errorf("not JSON line: %q", l)
		}
	}
}

func TestRenderJSONPath(t *testing.T) {
	var buf bytes.Buffer
	r := &Renderer{Format: FormatJSONPath, JSONPath: ".queues[0].name", Out: &buf}
	if err := r.Render(sampleData()); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(buf.String()); got != "orders" {
		t.Errorf("got %q want orders", got)
	}
}

func TestRenderTableRespectsWide(t *testing.T) {
	view := View{
		Columns: []Column{
			{Header: "NAME"},
			{Header: "EXTRA", Wide: true},
		},
	}
	row := map[string]any{"name": "x", "extra": "secret"}
	var narrow, wide bytes.Buffer
	(&Renderer{Format: FormatTable, View: view, Out: &narrow}).Render(row)
	(&Renderer{Format: FormatWide, View: view, Out: &wide}).Render(row)
	if strings.Contains(narrow.String(), "secret") {
		t.Error("narrow table should hide Wide column")
	}
	if !strings.Contains(wide.String(), "secret") {
		t.Error("wide table should show Wide column")
	}
}
