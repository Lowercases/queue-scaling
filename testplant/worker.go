package testplant

import (
	"math"
	"math/rand"
	"time"
)

const MAX_P_UNIT = 30000

type Worker struct {
	done      chan bool
	queue     *Queue
	processed chan uint
}

func NewWorker(q *Queue, p chan uint, mu_p0, sigma_p0 uint, unit time.Duration) *Worker {
	w := &Worker{
		queue:     q,
		processed: p,
		done:      make(chan bool),
	}

	go w.run(mu_p0, sigma_p0, unit)

	return w
}

func (w *Worker) Kill() {
	w.done <- true
}

func (w *Worker) run(mu_p0, sigma_p0 uint, unit time.Duration) {
	for {
		select {
		case <-w.done:
			return

		case <-w.queue.Recv:
			d := process(mu_p0, sigma_p0, unit)
			w.processed <- d
		}
	}
}

func process(mu_p0, sigma_p0 uint, unit time.Duration) uint {
	r := uint(math.Exp(rand.NormFloat64()*float64(sigma_p0) + float64(mu_p0)))
	if r > MAX_P_UNIT {
		return MAX_P_UNIT
	}
	// Increase precision for sleep
	sl := float64(r) * float64(unit/time.Nanosecond)
	time.Sleep(time.Duration(sl))
	return r
}
