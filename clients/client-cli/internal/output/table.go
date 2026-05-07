package output

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/dustin/go-humanize"
)

// renderTable emits a fixed-width, ANSI-aware table. When wide is true, all
// declared columns (including ones marked Wide) are shown.
func (r *Renderer) renderTable(data any, wide bool) error {
	view := r.View
	if len(view.Columns) == 0 {
		// No declared view: fall through to JSON to avoid surprising the
		// caller with garbled output.
		return r.renderJSON(data, true)
	}

	cols := pickColumns(view.Columns, wide)
	rows := extractRows(view, data)

	cells := make([][]string, 0, len(rows))
	for _, row := range rows {
		line := make([]string, len(cols))
		for ci, col := range cols {
			line[ci] = renderCell(row, col)
		}
		cells = append(cells, line)
	}

	widths := make([]int, len(cols))
	for ci, col := range cols {
		widths[ci] = displayWidth(col.Header)
	}
	for _, line := range cells {
		for ci, v := range line {
			if w := displayWidth(v); w > widths[ci] {
				widths[ci] = w
			}
		}
	}

	headStyle := lipgloss.NewStyle()
	if r.Color {
		headStyle = headStyle.Bold(true)
	}

	var sb strings.Builder
	if !r.NoHeaders {
		for ci, col := range cols {
			if ci > 0 {
				sb.WriteString("  ")
			}
			sb.WriteString(headStyle.Render(padRight(col.Header, widths[ci])))
		}
		sb.WriteByte('\n')
	}
	for _, line := range cells {
		for ci, v := range line {
			if ci > 0 {
				sb.WriteString("  ")
			}
			sb.WriteString(padRight(v, widths[ci]))
		}
		sb.WriteByte('\n')
	}

	_, err := fmt.Fprint(r.Out, sb.String())
	return err
}

func pickColumns(in []Column, wide bool) []Column {
	out := make([]Column, 0, len(in))
	for _, c := range in {
		if c.Wide && !wide {
			continue
		}
		out = append(out, c)
	}
	return out
}

func extractRows(v View, data any) []any {
	if v.RowsFrom != nil {
		rows := v.RowsFrom(data)
		if rows == nil {
			return []any{}
		}
		return rows
	}
	// Treat single object as one row.
	return []any{data}
}

func renderCell(row any, col Column) string {
	if col.Format != nil {
		return col.Format(resolve(row, col))
	}
	return defaultFormat(resolve(row, col))
}

func resolve(row any, col Column) any {
	if col.Path != "" {
		v, _ := evalJSONPath(row, col.Path)
		return v
	}
	m, ok := row.(map[string]any)
	if !ok {
		return row
	}
	// Try header lower-cased then in original case.
	for _, key := range []string{strings.ToLower(col.Header), col.Header} {
		if v, ok := m[key]; ok {
			return v
		}
	}
	return nil
}

func defaultFormat(v any) string {
	switch x := v.(type) {
	case nil:
		return "-"
	case string:
		if x == "" {
			return "-"
		}
		// Try parsing as time; if it works, render relative.
		if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return humanize.Time(t)
		}
		if t, err := time.Parse(time.RFC3339, x); err == nil {
			return humanize.Time(t)
		}
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case []any:
		return strconv.Itoa(len(x))
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return "{" + strings.Join(keys, ",") + "}"
	default:
		return fmt.Sprintf("%v", x)
	}
}

func displayWidth(s string) int { return len([]rune(s)) }

func padRight(s string, w int) string {
	if displayWidth(s) >= w {
		return s
	}
	return s + strings.Repeat(" ", w-displayWidth(s))
}

// FormatBytes returns a humanised byte count.
func FormatBytes(v any) string {
	switch x := v.(type) {
	case float64:
		return humanize.Bytes(uint64(x))
	case int:
		return humanize.Bytes(uint64(x))
	case int64:
		return humanize.Bytes(uint64(x))
	default:
		return defaultFormat(v)
	}
}
