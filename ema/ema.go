package ema

import "math"

// Implement an exponentially moving average to the likes of the writer. Because
// there's millions of implementations so what's wrong with yet another one.
type EMA struct {
	history []float64
	size    int

	// Cache for quick computing
	e_minus_tau float64

	// For exponentially moving integrals
	integral bool
}

// New Exponentially Moving Average. The argument is the amount of samples we'll
// keep, and it's defined as the number for which the n-th old sample would have
// a weight of 5%, the point at which we discard it. The 0-th sample has got a
// weight of 1.
func NewEMA(n_p95 int) *EMA {
	return newema(n_p95, false)
}

// New Exponentially Moving Integral. Works the same as the EMA does, but
// without dividing by the total weight at the end.
func NewEMI(n_p95 int) *EMA {
	return newema(n_p95, true)
}

func newema(n_p95 int, integral bool) *EMA {
	if n_p95 < 1 {
		panic("Size must be positive.")
	}
	return &EMA{
		history:     make([]float64, 0, n_p95),
		size:        n_p95,
		e_minus_tau: math.Pow(1.0/20.0, 1.0/float64(n_p95)),
		integral:    integral,
	}
}

func (ema *EMA) Add(value float64) {
	if len(ema.history) == ema.size {
		ema.history = ema.history[1:ema.size]
	}
	ema.history = append(ema.history, value)

}

func (ema *EMA) Value() float64 {
	var value float64

	L := len(ema.history)
	if L == 0 {
		return 0.0 // Likely more useful than panicking
	}

	weight, total_weight := 1.0, 0.0
	for i := L - 1; i >= 0; i-- {
		value += ema.history[i] * weight
		total_weight += weight
		weight *= ema.e_minus_tau
	}

	if !ema.integral {
		value /= total_weight
	}
	return value

}
