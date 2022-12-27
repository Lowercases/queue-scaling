package ema

import (
	"math"
	"testing"
)

func kindaEqual(a, b float64) bool {
	if b > a {
		return kindaEqual(b, a)
	}
	return a-b < 1e-9
}

func TestExponentialMovingAverageFilter(t *testing.T) {
	avg := NewEMA(5)

	e_tau := math.Pow(1.0/20, 1.0/5)

	inputs := []float64{1, 1, 2, 3, 5, 8, 13, 21, 34, -55, -89, -144}
	make_output := func(idx int) float64 {
		totes, weight := 0.0, 0.0
		j := 0
		for i := 0; i <= idx; i++ {
			if idx-i >= 5 {
				continue
			}
			totes *= e_tau
			totes += inputs[i]
			weight += math.Pow(e_tau, float64(j))
			j++
		}
		return totes / weight
	}

	for i := range inputs {
		avg.Add(inputs[i])
		current := avg.Value()
		output := make_output(i)
		if !kindaEqual(current, output) {
			t.Errorf("%d: expected filter output %v, got %v", i, output, current)
		}
	}
}
