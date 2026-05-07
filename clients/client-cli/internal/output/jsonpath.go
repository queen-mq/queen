package output

import (
	"fmt"
	"strconv"
	"strings"
)

// renderJSONPath supports a small subset of jq-style paths sufficient for
// shell pipelines: dotted keys (".foo.bar") and array index ([N]). It is
// intentionally stricter than full JSONPath - we want predictable behaviour
// over feature parity.
func (r *Renderer) renderJSONPath(data any, expr string) error {
	if expr == "" {
		return fmt.Errorf("jsonpath expression is empty")
	}
	v, err := evalJSONPath(data, expr)
	if err != nil {
		return err
	}
	switch tv := v.(type) {
	case nil:
		_, err = fmt.Fprintln(r.Out)
	case string:
		_, err = fmt.Fprintln(r.Out, tv)
	case bool:
		_, err = fmt.Fprintln(r.Out, tv)
	case float64:
		_, err = fmt.Fprintln(r.Out, formatNumber(tv))
	case int, int64:
		_, err = fmt.Fprintln(r.Out, tv)
	default:
		return r.renderJSON(tv, true)
	}
	return err
}

func formatNumber(f float64) string {
	if f == float64(int64(f)) {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func evalJSONPath(data any, expr string) (any, error) {
	expr = strings.TrimSpace(expr)
	if expr == "." || expr == "" {
		return data, nil
	}
	if !strings.HasPrefix(expr, ".") && !strings.HasPrefix(expr, "[") {
		expr = "." + expr
	}
	cur := data
	i := 0
	for i < len(expr) {
		c := expr[i]
		switch c {
		case '.':
			j := i + 1
			for j < len(expr) && expr[j] != '.' && expr[j] != '[' {
				j++
			}
			key := expr[i+1 : j]
			if key == "" {
				return nil, fmt.Errorf("jsonpath: empty key at %d", i)
			}
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("jsonpath: cannot resolve %q on non-object", key)
			}
			cur = m[key]
			i = j
		case '[':
			j := strings.IndexByte(expr[i:], ']')
			if j < 0 {
				return nil, fmt.Errorf("jsonpath: unterminated [")
			}
			j += i
			idx, err := strconv.Atoi(expr[i+1 : j])
			if err != nil {
				return nil, fmt.Errorf("jsonpath: bad index %q", expr[i+1:j])
			}
			arr, ok := cur.([]any)
			if !ok {
				return nil, fmt.Errorf("jsonpath: cannot index non-array")
			}
			if idx < 0 || idx >= len(arr) {
				return nil, fmt.Errorf("jsonpath: index %d out of range (len=%d)", idx, len(arr))
			}
			cur = arr[idx]
			i = j + 1
		default:
			return nil, fmt.Errorf("jsonpath: unexpected %q at %d", c, i)
		}
	}
	return cur, nil
}
