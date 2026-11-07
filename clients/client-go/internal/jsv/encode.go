package jsv

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"unicode/utf8"
)

// Marshal writes v as JSON.stringify(v) does: no HTML escaping, JS number
// formatting, Undefined fields left out (null in a list), NaN and infinities
// as null. Ordered objects keep their key order; a Go map is written in key
// order (JS reads every value here by key, so only the chain hash, which is
// built from Ordered objects, depends on order).
func Marshal(v interface{}) ([]byte, error) {
	return appendValue(make([]byte, 0, 64), v, 0)
}

// MustMarshal is Marshal for values known to encode (built here).
func MustMarshal(v interface{}) []byte {
	b, err := Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// RawJSON is JSON text written into the output as it is.
type RawJSON []byte

const maxDepth = 512

func appendValue(b []byte, v interface{}, depth int) ([]byte, error) {
	if depth > maxDepth {
		return nil, fmt.Errorf("jsv: value nested too deep (a cycle?)")
	}
	switch x := v.(type) {
	case nil:
		return append(b, "null"...), nil
	case undefinedType:
		return append(b, "null"...), nil
	case bool:
		if x {
			return append(b, "true"...), nil
		}
		return append(b, "false"...), nil
	case string:
		return AppendString(b, x), nil
	case float64:
		return appendFloat(b, x), nil
	case RawJSON:
		return append(b, x...), nil
	case json.RawMessage:
		if len(x) == 0 {
			return append(b, "null"...), nil
		}
		// Normalized, so a raw value reads the same as one built here.
		dv, err := Unmarshal(x)
		if err != nil {
			return nil, err
		}
		return appendValue(b, dv, depth+1)
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return nil, err
		}
		return appendFloat(b, f), nil
	case map[string]interface{}:
		keys := make([]string, 0, len(x))
		for k, e := range x {
			if !IsUndefined(e) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		b = append(b, '{')
		for i, k := range keys {
			if i > 0 {
				b = append(b, ',')
			}
			b = AppendString(b, k)
			b = append(b, ':')
			var err error
			if b, err = appendValue(b, x[k], depth+1); err != nil {
				return nil, err
			}
		}
		return append(b, '}'), nil
	case Ordered:
		b = append(b, '{')
		first := true
		for _, p := range x {
			if IsUndefined(p.V) {
				continue
			}
			if !first {
				b = append(b, ',')
			}
			first = false
			b = AppendString(b, p.K)
			b = append(b, ':')
			var err error
			if b, err = appendValue(b, p.V, depth+1); err != nil {
				return nil, err
			}
		}
		return append(b, '}'), nil
	case []interface{}:
		b = append(b, '[')
		for i, e := range x {
			if i > 0 {
				b = append(b, ',')
			}
			var err error
			if b, err = appendValue(b, e, depth+1); err != nil {
				return nil, err
			}
		}
		return append(b, ']'), nil
	case []string:
		b = append(b, '[')
		for i, e := range x {
			if i > 0 {
				b = append(b, ',')
			}
			b = AppendString(b, e)
		}
		return append(b, ']'), nil
	}
	if f, ok := Num(v); ok {
		return appendFloat(b, f), nil
	}
	if m, ok := v.(json.Marshaler); ok {
		raw, err := m.MarshalJSON()
		if err != nil {
			return nil, err
		}
		return appendValue(b, json.RawMessage(raw), depth+1)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.IsNil() {
			return append(b, "null"...), nil
		}
		b = append(b, '[')
		for i := 0; i < rv.Len(); i++ {
			if i > 0 {
				b = append(b, ',')
			}
			var err error
			if b, err = appendValue(b, rv.Index(i).Interface(), depth+1); err != nil {
				return nil, err
			}
		}
		return append(b, ']'), nil
	case reflect.Map:
		if rv.Type().Key().Kind() == reflect.String {
			if rv.IsNil() {
				return append(b, "null"...), nil
			}
			m := make(map[string]interface{}, rv.Len())
			it := rv.MapRange()
			for it.Next() {
				m[it.Key().String()] = it.Value().Interface()
			}
			return appendValue(b, m, depth+1)
		}
	case reflect.Ptr, reflect.Interface:
		if rv.IsNil() {
			return append(b, "null"...), nil
		}
		return appendValue(b, rv.Elem().Interface(), depth+1)
	}
	// Anything else (a struct with json tags, a time.Time): as encoding/json
	// sees it, then normalized.
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return appendValue(b, json.RawMessage(raw), depth+1)
}

func appendFloat(b []byte, f float64) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(b, "null"...)
	}
	return appendNumber(b, f)
}

const hexDigits = "0123456789abcdef"

// AppendString writes s as a JSON string the way JSON.stringify does.
func AppendString(b []byte, s string) []byte {
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			b = append(b, s[start:i]...)
			switch c {
			case '"':
				b = append(b, '\\', '"')
			case '\\':
				b = append(b, '\\', '\\')
			case '\b':
				b = append(b, '\\', 'b')
			case '\f':
				b = append(b, '\\', 'f')
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = append(b, s[start:i]...)
			b = append(b, `�`...)
			i += size
			start = i
			continue
		}
		i += size
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}
