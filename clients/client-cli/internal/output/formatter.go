package output

import (
	"fmt"
	"io"
	"strings"
)

// Format identifies an output rendering style.
type Format string

const (
	FormatTable    Format = "table"
	FormatJSON     Format = "json"
	FormatNDJSON   Format = "ndjson"
	FormatYAML     Format = "yaml"
	FormatJSONPath Format = "jsonpath"
	FormatWide     Format = "wide"
	FormatRaw      Format = "raw"
)

// ParseFormat normalises a -o flag value, supporting "jsonpath=.foo.bar"
// inline expressions.
func ParseFormat(raw string) (Format, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", nil
	}
	if eq := strings.IndexByte(raw, '='); eq >= 0 {
		head := strings.ToLower(raw[:eq])
		expr := raw[eq+1:]
		switch head {
		case "jsonpath", "json-path":
			return FormatJSONPath, expr, nil
		default:
			return "", "", fmt.Errorf("unknown output expression %q", head)
		}
	}
	switch strings.ToLower(raw) {
	case "table":
		return FormatTable, "", nil
	case "json":
		return FormatJSON, "", nil
	case "ndjson", "jsonl":
		return FormatNDJSON, "", nil
	case "yaml":
		return FormatYAML, "", nil
	case "wide":
		return FormatWide, "", nil
	case "raw":
		return FormatRaw, "", nil
	default:
		return "", "", fmt.Errorf("unknown output format %q", raw)
	}
}

// Column describes one column of a table view.
type Column struct {
	Header string
	// Path is a dotted JSON path into the input map. Empty Path means use
	// the top-level key matching Header (lower-cased) - useful for one-level
	// rows.
	Path string
	// Format optionally transforms the resolved value into a string. If nil,
	// a sensible default is used (humanised numbers, dates, durations).
	Format func(any) string
	// Wide is true if the column should only be shown with -o wide.
	Wide bool
}

// View is the per-command shape: column declaration + how to extract the
// row slice from the raw API response.
type View struct {
	Columns []Column
	// RowsFrom returns the slice of row maps from the raw API result. If
	// nil, the result itself is treated as a single row (object view).
	RowsFrom func(any) []any
}

// Renderer renders any data through one of the supported formats.
type Renderer struct {
	Format     Format
	JSONPath   string
	Color      bool
	NoHeaders  bool
	View       View
	Out        io.Writer
}

// Render is the single entry point used by every CLI command.
func (r *Renderer) Render(data any) error {
	switch r.Format {
	case "", FormatTable, FormatWide:
		return r.renderTable(data, r.Format == FormatWide)
	case FormatJSON:
		return r.renderJSON(data, true)
	case FormatNDJSON:
		return r.renderNDJSON(data)
	case FormatYAML:
		return r.renderYAML(data)
	case FormatJSONPath:
		return r.renderJSONPath(data, r.JSONPath)
	case FormatRaw:
		return r.renderRaw(data)
	default:
		return fmt.Errorf("unsupported format %q", r.Format)
	}
}

func (r *Renderer) renderRaw(data any) error {
	switch v := data.(type) {
	case string:
		_, err := io.WriteString(r.Out, v)
		return err
	case []byte:
		_, err := r.Out.Write(v)
		return err
	default:
		_, err := fmt.Fprintln(r.Out, v)
		return err
	}
}
