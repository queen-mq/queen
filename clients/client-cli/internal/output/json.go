package output

import (
	"encoding/json"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

func (r *Renderer) renderJSON(data any, pretty bool) error {
	enc := json.NewEncoder(r.Out)
	if pretty {
		enc.SetIndent("", "  ")
	}
	if r.View.RowsFrom != nil {
		return enc.Encode(r.View.RowsFrom(data))
	}
	return enc.Encode(data)
}

func (r *Renderer) renderNDJSON(data any) error {
	var rows []any
	if r.View.RowsFrom != nil {
		rows = r.View.RowsFrom(data)
	}
	if rows == nil {
		// Fall back: emit a single object on one line.
		return writeNDJSONLine(r.Out, data)
	}
	for _, row := range rows {
		if err := writeNDJSONLine(r.Out, row); err != nil {
			return err
		}
	}
	return nil
}

func writeNDJSONLine(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("encode ndjson: %w", err)
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	_, err = w.Write([]byte{'\n'})
	return err
}

func (r *Renderer) renderYAML(data any) error {
	enc := yaml.NewEncoder(r.Out)
	enc.SetIndent(2)
	defer enc.Close()
	if r.View.RowsFrom != nil {
		return enc.Encode(r.View.RowsFrom(data))
	}
	return enc.Encode(data)
}
