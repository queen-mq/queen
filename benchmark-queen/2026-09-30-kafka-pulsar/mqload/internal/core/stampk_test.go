package core

import (
	"encoding/json"
	"testing"
)

// -stamp-k: every message carries "k" = its index in the unit mod KeyMod, stays valid JSON, and still reads as a
// load message (ts and src first), so a where on data.k takes an exact share.
func TestStampKeyed(t *testing.T) {
	p := NewPayloadPool(1, 256, 200, 3)
	p.KeyMod = 100
	for round := 0; round < 3; round++ {
		out := p.Stamp(1758500000123456, 200)
		if len(out) != 200 {
			t.Fatalf("got %d payloads", len(out))
		}
		under30 := 0
		for j, m := range out {
			var v map[string]any
			if err := json.Unmarshal(m, &v); err != nil {
				t.Fatalf("message %d not JSON: %v: %s", j, err, m)
			}
			if k := int(v["k"].(float64)); k != j%100 {
				t.Fatalf("message %d: k=%d", j, k)
			}
			if int(v["k"].(float64)) < 30 {
				under30++
			}
			if _, ok := v["type"]; !ok {
				t.Fatalf("message %d lost the event's fields: %s", j, m)
			}
			ts, src, ok := ParseStamp(m)
			if !ok || ts != 1758500000123456 || src != 3 {
				t.Fatalf("message %d: ParseStamp = %d %d %v", j, ts, src, ok)
			}
		}
		if under30 != 60 {
			t.Fatalf("k < 30 taken by %d of 200, want 60", under30)
		}
	}
	p.KeyMod = 0
	m := p.Stamp(1, 5)[0]
	var v map[string]any
	if err := json.Unmarshal(m, &v); err != nil || v["k"] != nil {
		t.Fatalf("KeyMod 0 must not add k: %s", m)
	}
}
