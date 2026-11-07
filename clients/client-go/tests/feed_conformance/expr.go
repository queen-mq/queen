package main

// The expression language of the conformance cases (README section 3), as
// clients/conformance/feed/runners/js.mjs evaluates it.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/smartpricing/queen/clients/client-go/v2/internal/jsv"
)

// decodeOrdered decodes JSON keeping the key order of objects (jsv.Ordered).
func decodeOrdered(data []byte) (interface{}, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	v, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (interface{}, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			o := jsv.Ordered{}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				k, _ := kt.(string)
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				o = append(o, jsv.Pair{K: k, V: v})
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return o, nil
		case '[':
			l := []interface{}{}
			for dec.More() {
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				l = append(l, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return l, nil
		}
		return nil, fmt.Errorf("unexpected %v", t)
	}
	return tok, nil
}

// plain turns ordered objects into maps (where order does not matter).
func plain(v interface{}) interface{} {
	switch x := v.(type) {
	case jsv.Ordered:
		m := make(map[string]interface{}, len(x))
		for _, p := range x {
			m[p.K] = plain(p.V)
		}
		return m
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, e := range x {
			out[i] = plain(e)
		}
		return out
	}
	return v
}

var flaky = struct {
	sync.Mutex
	n map[string]int
}{n: map[string]int{}}

func utf16Len(s string) int {
	n := 0
	for _, r := range s {
		if r >= 0x10000 {
			n += 2
		} else {
			n++
		}
	}
	if !utf8.ValidString(s) {
		return len(s)
	}
	return n
}

func deepEq(a, b interface{}) bool {
	if jsv.IsUndefined(a) || jsv.IsUndefined(b) {
		return jsv.IsUndefined(a) && jsv.IsUndefined(b)
	}
	if a == nil || b == nil || !jsv.IsObject(a) || !jsv.IsObject(b) {
		return jsv.Same(a, b)
	}
	la, aList := jsv.Normalize(a).([]interface{})
	lb, bList := jsv.Normalize(b).([]interface{})
	if aList != bList {
		return false
	}
	if aList {
		if len(la) != len(lb) {
			return false
		}
		for i := range la {
			if !deepEq(la[i], lb[i]) {
				return false
			}
		}
		return true
	}
	ma, _ := jsv.Normalize(a).(map[string]interface{})
	mb, _ := jsv.Normalize(b).(map[string]interface{})
	ka, kb := 0, 0
	for k, v := range ma {
		if jsv.IsUndefined(v) {
			continue
		}
		ka++
		w, ok := mb[k]
		if !ok || !deepEq(v, w) {
			return false
		}
	}
	for _, v := range mb {
		if !jsv.IsUndefined(v) {
			kb++
		}
	}
	return ka == kb
}

func getPath(args []interface{}, path interface{}) interface{} {
	segs := strings.Split(jsv.String(path), ".")
	i, err := strconv.Atoi(segs[0])
	if err != nil || i < 0 || i >= len(args) {
		return jsv.Undefined
	}
	cur := args[i]
	for _, seg := range segs[1:] {
		switch c := cur.(type) {
		case map[string]interface{}:
			v, ok := c[seg]
			if !ok {
				return jsv.Undefined
			}
			cur = v
		case []interface{}:
			n, err := strconv.Atoi(seg)
			if err != nil || n < 0 || n >= len(c) {
				return jsv.Undefined
			}
			cur = c[n]
		default:
			if jsv.IsObject(cur) {
				norm := jsv.Normalize(cur)
				if m, ok := norm.(map[string]interface{}); ok {
					v, ok := m[seg]
					if !ok {
						return jsv.Undefined
					}
					cur = v
					continue
				}
			}
			return jsv.Undefined
		}
	}
	return cur
}

func cmp(a, b interface{}) (int, bool) {
	if fa, ok := jsv.Num(a); ok {
		fb, ok := jsv.Num(b)
		if !ok {
			return 0, false
		}
		switch {
		case fa < fb:
			return -1, true
		case fa > fb:
			return 1, true
		}
		return 0, true
	}
	sa, ok1 := a.(string)
	sb, ok2 := b.(string)
	if ok1 && ok2 {
		return strings.Compare(sa, sb), true
	}
	return 0, false
}

// evaluate is the value of expr with the function's arguments.
func evaluate(expr interface{}, args []interface{}) (interface{}, error) {
	switch e := expr.(type) {
	case []interface{}:
		out := make([]interface{}, len(e))
		for i, x := range e {
			v, err := evaluate(x, args)
			if err != nil {
				return nil, err
			}
			if jsv.IsUndefined(v) {
				v = nil
			}
			out[i] = v
		}
		return out, nil
	case jsv.Ordered:
		op := ""
		for _, p := range e {
			if strings.HasPrefix(p.K, "$") {
				op = p.K
				break
			}
		}
		if op == "" {
			out := map[string]interface{}{}
			for _, p := range e {
				v, err := evaluate(p.V, args)
				if err != nil {
					return nil, err
				}
				if !jsv.IsUndefined(v) {
					out[p.K] = v
				}
			}
			return out, nil
		}
		if len(e) != 1 {
			return nil, fmt.Errorf("expression: %s stands alone in its object", op)
		}
		return evalOp(op, e[0].V, args)
	}
	return expr, nil
}

func list(a interface{}) []interface{} {
	l, _ := a.([]interface{})
	return l
}

func at(l []interface{}, i int) interface{} {
	if i < len(l) {
		return l[i]
	}
	return jsv.Undefined
}

func evalOp(op string, a interface{}, args []interface{}) (interface{}, error) {
	ev := func(x interface{}) (interface{}, error) { return evaluate(x, args) }
	two := func() (interface{}, interface{}, error) {
		l := list(a)
		x, err := ev(at(l, 0))
		if err != nil {
			return nil, nil, err
		}
		y, err := ev(at(l, 1))
		return x, y, err
	}
	switch op {
	case "$get":
		return getPath(args, a), nil
	case "$eq", "$ne":
		x, y, err := two()
		if err != nil {
			return nil, err
		}
		r := deepEq(x, y)
		if op == "$ne" {
			r = !r
		}
		return r, nil
	case "$lt", "$lte", "$gt", "$gte":
		x, y, err := two()
		if err != nil {
			return nil, err
		}
		c, ok := cmp(x, y)
		if !ok {
			return false, nil
		}
		switch op {
		case "$lt":
			return c < 0, nil
		case "$lte":
			return c <= 0, nil
		case "$gt":
			return c > 0, nil
		}
		return c >= 0, nil
	case "$and":
		for _, x := range list(a) {
			v, err := ev(x)
			if err != nil {
				return nil, err
			}
			if !jsv.Truthy(v) {
				return false, nil
			}
		}
		return true, nil
	case "$or":
		for _, x := range list(a) {
			v, err := ev(x)
			if err != nil {
				return nil, err
			}
			if jsv.Truthy(v) {
				return true, nil
			}
		}
		return false, nil
	case "$not":
		v, err := ev(a)
		if err != nil {
			return nil, err
		}
		return !jsv.Truthy(v), nil
	case "$if":
		l := list(a)
		c, err := ev(at(l, 0))
		if err != nil {
			return nil, err
		}
		if jsv.Truthy(c) {
			return ev(at(l, 1))
		}
		return ev(at(l, 2))
	case "$add":
		s := 0.0
		for _, x := range list(a) {
			v, err := ev(x)
			if err != nil {
				return nil, err
			}
			if f, ok := jsv.Num(v); ok {
				s += f
			}
		}
		return s, nil
	case "$mod":
		x, y, err := two()
		if err != nil {
			return nil, err
		}
		fx, ok1 := jsv.Num(x)
		fy, ok2 := jsv.Num(y)
		if !ok1 || !ok2 {
			return math.NaN(), nil
		}
		return math.Mod(fx, fy), nil
	case "$str":
		v, err := ev(a)
		if err != nil {
			return nil, err
		}
		return jsv.String(v), nil
	case "$concat":
		var sb strings.Builder
		for _, x := range list(a) {
			v, err := ev(x)
			if err != nil {
				return nil, err
			}
			if !jsv.IsUndefined(v) {
				sb.WriteString(jsv.String(v))
			}
		}
		return sb.String(), nil
	case "$len":
		v, err := ev(a)
		if err != nil {
			return nil, err
		}
		if s, ok := v.(string); ok {
			return float64(utf16Len(s)), nil
		}
		if l, ok := jsv.Normalize(v).([]interface{}); ok && jsv.IsArray(v) {
			return float64(len(l)), nil
		}
		return 0.0, nil
	case "$keys":
		v, err := ev(a)
		if err != nil {
			return nil, err
		}
		keys := []interface{}{}
		if m, ok := jsv.Normalize(v).(map[string]interface{}); ok && !jsv.IsArray(v) {
			var ks []string
			for k, x := range m {
				if !jsv.IsUndefined(x) {
					ks = append(ks, k)
				}
			}
			sort.Strings(ks)
			for _, k := range ks {
				keys = append(keys, k)
			}
		}
		return keys, nil
	case "$missing":
		return jsv.Undefined, nil
	case "$throw":
		return nil, errors.New(jsv.String(plain(a)))
	case "$flaky":
		spec, _ := a.(jsv.Ordered)
		kv, err := ev(spec.Get("key"))
		if err != nil {
			return nil, err
		}
		key := jsv.String(kv)
		times, _ := jsv.Num(spec.Get("times"))
		flaky.Lock()
		n := flaky.n[key]
		if float64(n) < times {
			flaky.n[key] = n + 1
			flaky.Unlock()
			return nil, fmt.Errorf("flaky %s %d", key, n+1)
		}
		flaky.Unlock()
		return ev(spec.Get("then"))
	}
	return nil, fmt.Errorf("expression: unknown operator %s", op)
}
