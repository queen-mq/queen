// Package jsv holds the JavaScript value rules the feed streams share with the
// JS SDK: undefined, ===, truthiness, String(v), JSON.stringify's bytes,
// Date.parse and Date.prototype.toISOString, and the SDK's duration parser.
//
// A feed stream must behave the same in every SDK and keep byte-compatible
// state (a JS runner stops, a Go runner with the same queryId goes on), so the
// Go engine computes on JS values: decoded JSON (map[string]interface{},
// []interface{}, float64, string, bool, nil) plus Undefined, a field that is
// not there. Times are float64 milliseconds, as JS numbers are.
package jsv

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
)

type undefinedType struct{}

// Undefined is JavaScript's undefined: a value that is not there. A map field
// holding it is left out of JSON, an array item holding it is null (as
// JSON.stringify does).
var Undefined interface{} = undefinedType{}

// IsUndefined reports whether v is Undefined.
func IsUndefined(v interface{}) bool {
	_, ok := v.(undefinedType)
	return ok
}

// IsNullish is JS `v === null || v === undefined`.
func IsNullish(v interface{}) bool {
	return v == nil || IsUndefined(v)
}

// Pair is one key of an Ordered object.
type Pair struct {
	K string
	V interface{}
}

// Ordered is an object whose keys keep the order they were written in, as a
// JS object literal does. Used where bytes matter (the chain hash).
type Ordered []Pair

// Get answers the value of key k (Undefined when absent).
func (o Ordered) Get(k string) interface{} {
	for i := len(o) - 1; i >= 0; i-- {
		if o[i].K == k {
			return o[i].V
		}
	}
	return Undefined
}

// Set writes k: an existing key keeps its place (as a JS spread does), a new one goes last.
func (o Ordered) Set(k string, v interface{}) Ordered {
	for i := range o {
		if o[i].K == k {
			o[i].V = v
			return o
		}
	}
	return append(o, Pair{k, v})
}

// Num reads a JS number out of v: (value, true) for any Go number kind.
func Num(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int16:
		return float64(n), true
	case int8:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint64:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint8:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

// IsNumber is `typeof v === 'number'`.
func IsNumber(v interface{}) bool {
	_, ok := Num(v)
	return ok
}

// IsFinite is Number.isFinite(v).
func IsFinite(v interface{}) bool {
	f, ok := Num(v)
	return ok && !math.IsNaN(f) && !math.IsInf(f, 0)
}

// IsInteger is Number.isInteger(v).
func IsInteger(v interface{}) bool {
	f, ok := Num(v)
	return ok && !math.IsNaN(f) && !math.IsInf(f, 0) && f == math.Trunc(f)
}

// IsObject is `v !== null && typeof v === 'object'` for decoded JSON (maps and lists).
func IsObject(v interface{}) bool {
	switch v.(type) {
	case map[string]interface{}, []interface{}, Ordered:
		return true
	}
	if v == nil || IsUndefined(v) {
		return false
	}
	k := reflect.ValueOf(v).Kind()
	return k == reflect.Map || k == reflect.Slice || k == reflect.Struct || k == reflect.Ptr
}

// IsArray is Array.isArray(v).
func IsArray(v interface{}) bool {
	if _, ok := v.([]interface{}); ok {
		return true
	}
	if v == nil || IsUndefined(v) {
		return false
	}
	if _, ok := v.(json.RawMessage); ok {
		return false
	}
	return reflect.ValueOf(v).Kind() == reflect.Slice
}

// Same is JavaScript's ===: numbers and strings by value, objects by identity.
func Same(a, b interface{}) bool {
	if IsUndefined(a) || IsUndefined(b) {
		return IsUndefined(a) && IsUndefined(b)
	}
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if fa, ok := Num(a); ok {
		fb, ok := Num(b)
		return ok && fa == fb
	}
	switch x := a.(type) {
	case string:
		y, ok := b.(string)
		return ok && x == y
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	}
	va, vb := reflect.ValueOf(a), reflect.ValueOf(b)
	if va.Type() != vb.Type() {
		return false
	}
	switch va.Kind() {
	case reflect.Map, reflect.Ptr, reflect.Func, reflect.Chan:
		return va.Pointer() == vb.Pointer()
	case reflect.Slice:
		return va.Len() == vb.Len() && (va.Len() == 0 && va.Pointer() == vb.Pointer() || va.Len() > 0 && va.Pointer() == vb.Pointer())
	}
	return false
}

// Truthy is JavaScript's truthiness: false, 0, NaN, "", null and undefined are false.
func Truthy(v interface{}) bool {
	if v == nil || IsUndefined(v) {
		return false
	}
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return x != ""
	}
	if f, ok := Num(v); ok {
		return f != 0 && !math.IsNaN(f)
	}
	return true
}

// Clone is structuredClone for decoded JSON: maps and lists copied deep,
// everything else as it is (Undefined stays Undefined).
func Clone(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, e := range x {
			out[k] = Clone(e)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, e := range x {
			out[i] = Clone(e)
		}
		return out
	case Ordered:
		out := make(Ordered, len(x))
		for i, p := range x {
			out[i] = Pair{p.K, Clone(p.V)}
		}
		return out
	}
	return v
}

// JSONClone is JSON.parse(JSON.stringify(v)): a deep copy without the
// undefined fields, numbers as float64. Anything that does not encode comes
// back as it went in.
func JSONClone(v interface{}) interface{} {
	b, err := Marshal(v)
	if err != nil {
		return v
	}
	out, err := Unmarshal(b)
	if err != nil {
		return v
	}
	return out
}

// Unmarshal decodes JSON into decoded-JSON values (numbers float64).
func Unmarshal(b []byte) (interface{}, error) {
	var out interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// Normalize turns any Go value into decoded JSON (what a JSON round trip
// gives), keeping Undefined where it stands. Maps and lists of decoded JSON
// pass through untouched; other kinds (structs, typed slices, ints) go
// through encoding/json.
func Normalize(v interface{}) interface{} {
	switch x := v.(type) {
	case nil, bool, string, float64:
		return v
	case undefinedType:
		return v
	case map[string]interface{}, []interface{}:
		return v
	case Ordered:
		m := make(map[string]interface{}, len(x))
		for _, p := range x {
			m[p.K] = Normalize(p.V)
		}
		return m
	}
	if f, ok := Num(v); ok {
		return f
	}
	b, err := Marshal(v)
	if err != nil {
		return v
	}
	out, err := Unmarshal(b)
	if err != nil {
		return v
	}
	return out
}

// Get reads field k of an object (Undefined when v is no object or has no k).
func Get(v interface{}, k string) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		if e, ok := x[k]; ok {
			return e
		}
		return Undefined
	case Ordered:
		return x.Get(k)
	}
	return Undefined
}

// Equal is deep equality of JSON values (the conformance kit's $eq): Undefined
// equals only Undefined; a map field holding Undefined counts as absent.
func Equal(a, b interface{}) bool {
	if IsUndefined(a) || IsUndefined(b) {
		return IsUndefined(a) && IsUndefined(b)
	}
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if fa, ok := Num(a); ok {
		fb, ok := Num(b)
		return ok && fa == fb
	}
	switch x := a.(type) {
	case string:
		y, ok := b.(string)
		return ok && x == y
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	}
	a, b = Normalize(a), Normalize(b)
	la, aList := a.([]interface{})
	lb, bList := b.([]interface{})
	if aList || bList {
		if !aList || !bList || len(la) != len(lb) {
			return false
		}
		for i := range la {
			if !Equal(la[i], lb[i]) {
				return false
			}
		}
		return true
	}
	ma, aMap := a.(map[string]interface{})
	mb, bMap := b.(map[string]interface{})
	if !aMap || !bMap {
		return false
	}
	na, nb := 0, 0
	for k, v := range ma {
		if IsUndefined(v) {
			continue
		}
		na++
		w, ok := mb[k]
		if !ok || !Equal(v, w) {
			return false
		}
	}
	for _, v := range mb {
		if !IsUndefined(v) {
			nb++
		}
	}
	return na == nb
}

// String is JavaScript's String(v).
func String(v interface{}) string {
	if IsUndefined(v) {
		return "undefined"
	}
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case []interface{}:
		return joinList(x)
	case map[string]interface{}, Ordered:
		return "[object Object]"
	}
	if f, ok := Num(v); ok {
		return NumberString(f)
	}
	if IsArray(v) {
		if l, ok := Normalize(v).([]interface{}); ok {
			return joinList(l)
		}
	}
	return "[object Object]"
}

func joinList(l []interface{}) string {
	out := make([]byte, 0, 16)
	for i, e := range l {
		if i > 0 {
			out = append(out, ',')
		}
		if !IsNullish(e) {
			out = append(out, String(e)...)
		}
	}
	return string(out)
}

// NumberString is Number.prototype.toString(): `60` never `60.0`, exponents
// past 1e21 and below 1e-6.
func NumberString(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case f == 0:
		return "0"
	}
	return string(appendNumber(nil, f))
}

// appendNumber writes f as JSON.stringify does (encoding/json's float rule
// is ES6's Number.prototype.toString).
func appendNumber(b []byte, f float64) []byte {
	if f == 0 {
		return append(b, '0')
	}
	abs := math.Abs(f)
	format := byte('f')
	if abs < 1e-6 || abs >= 1e21 {
		format = 'e'
	}
	start := len(b)
	b = strconv.AppendFloat(b, f, format, -1, 64)
	if format == 'e' {
		// e-09 -> e-9, e+07 -> e+7
		n := len(b) - start
		if n >= 4 && b[len(b)-4] == 'e' && b[len(b)-2] == '0' {
			b[len(b)-2] = b[len(b)-1]
			b = b[:len(b)-1]
		}
	}
	return b
}
