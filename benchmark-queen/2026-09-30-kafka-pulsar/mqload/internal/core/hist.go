package core

// Latency histograms: goload's olHist, copied VERBATIM from
// benchmark-queen/.../mqload/ref/goload-pe-build/main.go (runOpenLoopMode's
// histogram) so kload/pload percentiles are bucket-identical with Queen's
// goload. Do not change the layout: report scripts merge the sparse bucket
// counts of several loaders (and of goload) by bucket index.

import (
	"math"
	"math/bits"
	"sync/atomic"
)

// Percentiles are computed by the reporter. Interval-local percentiles come
// from snapshot DIFFERENCING (snapshot() into a scratch slice, subtract the
// previous snapshot) rather than resetting the live buckets — so producers
// never race a reset and never pay for one. The same buckets also yield the
// cumulative (whole-run) percentiles for the final line.
const (
	olLinearMax  = 1024           // µs; unit-resolution region [0,1024)
	olSubBits    = 6              // 2^6 = 64 sub-buckets per octave
	olSubCount   = 1 << olSubBits // 64
	olBaseOctave = 10             // log2(olLinearMax)
	olMaxOctave  = 26             // 2^26 µs ≈ 67.1s ceiling
	olNumBuckets = olLinearMax + (olMaxOctave-olBaseOctave+1)*olSubCount
)

type olHist struct {
	buckets []int64
}

func newOLHist() *olHist { return &olHist{buckets: make([]int64, olNumBuckets)} }

// olBucketIndex maps a microsecond latency to its bucket index.
func olBucketIndex(v int64) int {
	if v <= 0 {
		return 0
	}
	if v < olLinearMax {
		return int(v)
	}
	octave := bits.Len64(uint64(v)) - 1 // floor(log2 v)
	if octave > olMaxOctave {
		return olNumBuckets - 1
	}
	shift := uint(octave - olSubBits)
	sub := int((v - (int64(1) << uint(octave))) >> shift) // 0..olSubCount-1
	return olLinearMax + (octave-olBaseOctave)*olSubCount + sub
}

func (h *olHist) record(v int64) { atomic.AddInt64(&h.buckets[olBucketIndex(v)], 1) }

// snapshot copies the current counts into dst (len must be olNumBuckets).
func (h *olHist) snapshot(dst []int64) {
	for i := range h.buckets {
		dst[i] = atomic.LoadInt64(&h.buckets[i])
	}
}

// olBucketMid returns the representative value (µs) of a bucket: the midpoint
// of the value range it covers. Used to turn a bucket index back into a latency.
func olBucketMid(idx int) float64 {
	if idx < olLinearMax {
		return float64(idx) + 0.5
	}
	j := idx - olLinearMax
	octave := olBaseOctave + j/olSubCount
	sub := j % olSubCount
	width := int64(1) << uint(octave-olSubBits)
	lo := (int64(1) << uint(octave)) + int64(sub)*width
	return float64(lo) + float64(width)/2
}

// olPercentile returns the p-th percentile (p in (0,1]) of a counts slice, in
// milliseconds. counts is either a cumulative snapshot or an interval diff.
func olPercentile(counts []int64, p float64) float64 {
	var total int64
	for _, c := range counts {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := int64(math.Ceil(p * float64(total)))
	if target < 1 {
		target = 1
	}
	var cum int64
	for i, c := range counts {
		cum += c
		if cum >= target {
			return olBucketMid(i) / 1000.0
		}
	}
	return olBucketMid(len(counts)-1) / 1000.0
}

// ---------------------------------------------------------------------------
// Helpers around the verbatim histogram (not in goload).

// sumSnapshot adds the counts of every histogram in hs into dst (zeroed first).
func sumSnapshot(dst []int64, hs []*olHist) {
	for i := range dst {
		dst[i] = 0
	}
	for _, h := range hs {
		for i := range h.buckets {
			dst[i] += atomic.LoadInt64(&h.buckets[i])
		}
	}
}

// countOf returns the number of samples in a counts slice.
func countOf(c []int64) int64 {
	var n int64
	for _, v := range c {
		n += v
	}
	return n
}

// maxOf returns the representative value (ms) of the highest non-empty bucket.
func maxOf(c []int64) float64 {
	for i := len(c) - 1; i >= 0; i-- {
		if c[i] > 0 {
			return olBucketMid(i) / 1000.0
		}
	}
	return 0
}

// sparse keeps the non-empty buckets (bucket index -> count) for the JSON
// result, so loaders (and goload) can be merged into exact percentiles.
func sparse(c []int64) map[int]int64 {
	m := map[int]int64{}
	for i, v := range c {
		if v > 0 {
			m[i] = v
		}
	}
	return m
}
