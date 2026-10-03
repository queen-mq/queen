package core

import (
	"math"
	"math/rand"
)

// Zipf draws ranks k in [1, n] with P(k) proportional to k^-s, for any s >= 0.
//
// Go's rand.Zipf needs s > 1, but the SPEC's topic weights default to s = 1.0,
// so this is the rejection-inversion sampler of Hörmann & Derflinger
// ("Rejection-inversion to generate variates from monotone discrete
// distributions", 1996), as implemented in Apache Commons RNG
// (RejectionInversionZipfSampler): O(1) per draw, no tables, exact for s > 0.
// s == 0 is uniform. Not safe for concurrent use (one per pacer worker).
type Zipf struct {
	rng         *rand.Rand
	n           float64
	s           float64
	hIntegralX1 float64
	hIntegralN  float64
	sq          float64
	uniform     bool
	nInt        int64
}

// NewZipf returns a sampler over [1, n] with exponent s (s >= 0).
func NewZipf(rng *rand.Rand, n uint64, s float64) *Zipf {
	if n < 1 {
		n = 1
	}
	z := &Zipf{rng: rng, n: float64(n), s: s, nInt: int64(n)}
	if s <= 0 {
		z.uniform = true
		return z
	}
	z.hIntegralX1 = z.hIntegral(1.5) - 1
	z.hIntegralN = z.hIntegral(z.n + 0.5)
	z.sq = 2 - z.hIntegralInverse(z.hIntegral(2.5)-z.h(2))
	return z
}

// Next returns a rank in [1, n].
func (z *Zipf) Next() uint64 {
	if z.uniform {
		return uint64(z.rng.Int63n(z.nInt)) + 1
	}
	for {
		u := z.hIntegralN + z.rng.Float64()*(z.hIntegralX1-z.hIntegralN)
		x := z.hIntegralInverse(u)
		k := math.Floor(x + 0.5)
		if k < 1 {
			k = 1
		} else if k > z.n {
			k = z.n
		}
		if k-x <= z.sq || u >= z.hIntegral(k+0.5)-z.h(k) {
			return uint64(k)
		}
	}
}

func (z *Zipf) hIntegral(x float64) float64 {
	logX := math.Log(x)
	return zipfHelper2((1-z.s)*logX) * logX
}

func (z *Zipf) h(x float64) float64 { return math.Exp(-z.s * math.Log(x)) }

func (z *Zipf) hIntegralInverse(x float64) float64 {
	t := x * (1 - z.s)
	if t < -1 {
		t = -1 // numerical guard, as in Commons RNG
	}
	return math.Exp(zipfHelper1(t) * x)
}

// zipfHelper1(x) = log1p(x)/x, with its Taylor series near 0.
func zipfHelper1(x float64) float64 {
	if math.Abs(x) > 1e-8 {
		return math.Log1p(x) / x
	}
	return 1 - x*(0.5-x*(1.0/3-0.25*x))
}

// zipfHelper2(x) = expm1(x)/x, with its Taylor series near 0.
func zipfHelper2(x float64) float64 {
	if math.Abs(x) > 1e-8 {
		return math.Expm1(x) / x
	}
	return 1 + x*0.5*(1+x*(1.0/3)*(1+0.25*x))
}
