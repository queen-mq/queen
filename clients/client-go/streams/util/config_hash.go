// Package util provides shared utilities for the queen-mq streaming SDK.
//
// config_hash — stable hash of an operator chain's structural shape. Mirror of
// the JS configHashOf and Python config_hash_of helpers. Cross-language
// consistency is required: the same logical chain must produce the same
// SHA-256 hex digest in JS, Python and Go so that a query registered in one
// language can be resumed by a worker written in another.
package util

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// Operator is the minimal interface needed to compute a config hash. To
// match the JS / Python reference implementations exactly, operators must
// expose two pieces:
//
//  1. Kind() — the structural kind name ("map", "window", "aggregate", ...).
//  2. ConfigKeys()/ConfigValue() — the config keys in INSERTION ORDER
//     plus a value lookup. Go's map[string]interface{} has no insertion
//     order, so passing keys explicitly is the only way to keep the hash
//     identical across languages.
//
// For backwards-compat, an operator may instead implement Config() returning
// a map[string]interface{}; in that case keys are sorted alphabetically (the
// hash will diverge from JS/Python — only safe for Go-only chains).
type Operator interface {
	Kind() string
	Config() map[string]interface{}
}

// OrderedConfigOperator is the preferred interface for cross-language
// hash compatibility. ConfigKeys() must return keys in JS insertion order.
type OrderedConfigOperator interface {
	Operator
	ConfigKeys() []string
}

// ConfigHashOf returns the SHA-256 hex digest of an operator chain's shape.
//
// CROSS-LANGUAGE COMPATIBILITY: the JSON wire format must match
//
//	JSON.stringify([{"kind": op.kind, ...op.config}, ...])  // JS
//	json.dumps([{"kind": op.kind, **op.config}, ...], separators=(',',':'))  // Python
//
// Both JS and Python preserve insertion order. Go does not. To stay 1:1 we
// build the JSON manually for each operator: "kind" first, then config keys
// in the order returned by ConfigKeys() (or alphabetical fallback).
func ConfigHashOf(operators []Operator) string {
	// Mirror of the JS describe(op) helper:
	//
	//   { kind: op.kind, ...op.config }
	//
	// JS spread overwrites earlier keys, so when op.config also has a "kind"
	// key the spread one wins (e.g. window operators set config.kind =
	// "window-tumbling" while op.kind = "window"). Python's `{**}` has the
	// same semantic. We replicate that by checking whether config has "kind"
	// up-front and emitting the OUTER kind only when the config doesn't.
	var buf []byte
	buf = append(buf, '[')
	for i, op := range operators {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '{')

		var keys []string
		var cfg map[string]interface{}
		if oc, ok := op.(OrderedConfigOperator); ok {
			keys = oc.ConfigKeys()
			cfg = oc.Config()
		} else if c := op.Config(); c != nil {
			cfg = c
			keys = make([]string, 0, len(c))
			for k := range c {
				keys = append(keys, k)
			}
			sort.Strings(keys)
		}

		first := true
		// 1. emit op.Kind() first — UNLESS config also has "kind", in which
		//    case the JS/Python spread overwrites it; we emit the config's
		//    kind value at config-key-iteration time so the bytes match.
		_, configHasKind := cfg["kind"]
		if !configHasKind {
			buf = append(buf, '"', 'k', 'i', 'n', 'd', '"', ':')
			kindJSON, _ := json.Marshal(op.Kind())
			buf = append(buf, kindJSON...)
			first = false
		}
		// 2. config keys in insertion order (or alphabetical fallback).
		for _, k := range keys {
			if !first {
				buf = append(buf, ',')
			}
			first = false
			kJSON, _ := json.Marshal(k)
			buf = append(buf, kJSON...)
			buf = append(buf, ':')
			vJSON, _ := json.Marshal(cfg[k])
			buf = append(buf, vJSON...)
		}
		buf = append(buf, '}')
	}
	buf = append(buf, ']')

	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}
