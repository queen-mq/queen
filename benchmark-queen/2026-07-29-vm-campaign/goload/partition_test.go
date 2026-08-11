package main

import "testing"

// The two properties the active-window policies exist to provide. Both are easy
// to break with an off-by-one in the modulo and neither fails loudly at runtime:
// a window that is one partition too wide, or a stride that revisits a subset,
// would still produce a plausible-looking run against a wrong workload.

func TestActiveWindowIsExactlyActiveWide(t *testing.T) {
	const space, active = 1_000_000, 1_000
	for _, policy := range []uint64{0, scatterMultiplier(space)} {
		for _, sec := range []uint64{0, 1, 7, 999, 1_000, 123_456} {
			seen := make(map[uint64]bool, active)
			// Ten requests per partition in the second, as a real run would do.
			for n := uint64(1); n <= active*10; n++ {
				seen[partitionIndex(policy, sec, n, active, space)] = true
			}
			if len(seen) != active {
				t.Fatalf("mult=%d sec=%d: %d distinct partitions, want %d", policy, sec, len(seen), active)
			}
		}
	}
}

func TestSpaceIsFullyCoveredInSpaceOverActiveSeconds(t *testing.T) {
	const space, active = 100_000, 100
	for _, policy := range []uint64{0, scatterMultiplier(space)} {
		seen := make(map[uint64]bool, space)
		for sec := uint64(0); sec < space/active; sec++ {
			for n := uint64(1); n <= active; n++ {
				seen[partitionIndex(policy, sec, n, active, space)] = true
			}
		}
		if len(seen) != space {
			t.Fatalf("mult=%d: covered %d of %d partitions in %d seconds", policy, len(seen), space, space/active)
		}
	}
}

// rotate keeps the window contiguous and scatter must not: the whole point of
// having both is that they land on the index differently.
func TestRotateIsContiguousAndScatterIsNot(t *testing.T) {
	const space, active = 1_000_000, 1_000
	var lo, hi uint64 = space, 0
	for n := uint64(1); n <= active; n++ {
		i := partitionIndex(0, 5, n, active, space)
		if i < lo {
			lo = i
		}
		if i > hi {
			hi = i
		}
	}
	if hi-lo != active-1 {
		t.Fatalf("rotate window spans %d, want %d", hi-lo+1, active)
	}

	lo, hi = space, 0
	for n := uint64(1); n <= active; n++ {
		i := partitionIndex(scatterMultiplier(space), 5, n, active, space)
		if i < lo {
			lo = i
		}
		if i > hi {
			hi = i
		}
	}
	if hi-lo < space/2 {
		t.Fatalf("scatter window spans only %d of %d, expected it spread across the space", hi-lo+1, space)
	}
}
