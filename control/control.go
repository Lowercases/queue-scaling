package control

import (
	"math"
	"time"

	"github.com/Lowercases/queue-scaling/ema"
)

type Manager interface {
	SetB() chan float64

	DXY(unit time.Duration) (float64, float64)
	XmY() uint // X - Y
	Q() uint
	Beta() uint

	// Optional for the plant since it can be got from Little's Law
	MuP() (float64, bool)
}

type Control struct {
	plant Manager

	dryRun bool

	// Settings
	t    uint
	mq   uint
	unit time.Duration

	// Queryable state
	dx, dy  float64 // derivatives (integral differences)
	r, b, k float64 // measured R and b and k setpoints
	xd      uint    // expected messages in the system
	u       float64 // utilisation of the workers' capacity

	// Busy workers integral and y estimation
	y, busyIntegral *ema.EMA

	// Exponential Moving Average for beta setting.
	betaEMA *ema.EMA

	// Internal concurrency, sampled as W / β on every active scenario, and
	// only while queued up. Both are lower bounds of the actual one, the
	// latter closer to it since the workers are then full.
	internalConcurrency *ema.EMA
	queuedConcurrency   *ema.EMA
}

// Lower bound for the utilisation when correcting R. W is a snapshot, so when
// only a few messages are in process its noise would make R / u blow up.
const minUtilisation = 0.1

func NewControl(plant Manager, controlPeriod, maxQueueTime uint, unit time.Duration) *Control {
	return &Control{
		plant:               plant,
		t:                   controlPeriod,
		mq:                  maxQueueTime,
		unit:                unit,
		betaEMA:             ema.NewEMA(1),
		y:                   ema.NewEMI(100),
		busyIntegral:        ema.NewEMI(100),
		internalConcurrency: ema.NewEMA(20),
		queuedConcurrency:   ema.NewEMA(20),
	}
}

func (c *Control) SetDryRun() {
	c.dryRun = true
}

func (c *Control) SetEMASize(size int) {
	c.betaEMA = ema.NewEMA(size)
}

func (c *Control) SetEMISize(size int) {
	c.y = ema.NewEMI(size)
	c.busyIntegral = ema.NewEMI(size)
}

func (c *Control) Run() {
	firstIteration := true

	for {
		time.Sleep(time.Duration(c.t) * c.unit)

		c.dx, c.dy = c.plant.DXY(c.unit)
		B := c.plant.Beta()
		Q := c.plant.Q()
		W := c.plant.XmY() - Q

		// Utilisation of the workers' capacity, i.e. W / β divided by their
		// internal concurrency. Since the internal concurrency is estimated
		// from lower bounds of it, this is an upper bound of the utilisation.
		c.u = 0
		if B > 0 {
			c.u = 1
			if Q <= B { // Not queued up
				c.u = math.Min(1, float64(W)/float64(B)/c.InternalConcurrency())
			}
		}

		// Busy workers, i.e. those R is measured against. Since the utilisation
		// is an upper bound, the correction might be partial but never
		// excessive. The utilisation is bounded below so as not to blow R up.
		// With nothing in process the workers are either idle, which shouldn't
		// count against R, or processing too quickly to be observed, in which
		// case the snapshot doesn't reflect the utilisation over the period
		// and we don't correct.
		busy := float64(B)
		if Q <= B { // Not queued up
			if W > 0 {
				busy *= math.Max(minUtilisation, c.u)
			} else if c.dy == 0 {
				busy = 0
			}
		}

		// Integrate busy workers and y. Practically speaking, in order to
		// integrate them we should multiply by the period; but since they are
		// always used as a ratio y / busyIntegral or compared against 0, we can
		// avoid that.
		c.y.Add(c.dy)            // * float64(c.t)
		c.busyIntegral.Add(busy) // * float64(c.t)

		if c.y.Value()*float64(c.t) < 1 || c.busyIntegral.Value() < 1 {
			// The system hasn't started yet. This is an arbitrary sane choice,
			// since we've got no point of reference -- we'll leave it alone if
			// greater than 0, and set it to 1 if there's data but it's scaled
			// to 0.
			if Q+W == 0 {
				// The system isn't receiving messages, so it's safe to stop it
				// or keep it stopped.
				c.b = 0
			} else if B > 0 {
				c.b = float64(B)
			} else {
				// Arbitrary choice. The system should self-correct as it learns
				// its processing rate.
				c.b = 1
			}

		} else { // X >= Y > 0
			// The system is operating with a non-zero input and output rate.
			// Compute the output throughput R, using the highest between
			// instant throughput (y-dot/beta) and historic (y/B), since the
			// latter is less exact but good when beta is close to zero, when
			// the workers are very likely to be starved.
			// Both count only the workers that were busy, so that idle ones
			// don't drag R down when the system is overscaled.
			R := c.y.Value() / c.busyIntegral.Value()
			if busy > 0 {
				RI := c.dy / busy
				if RI > R {
					R = RI
				}
			}

			// Save internal concurrency.
			if B > 0 {
				c.internalConcurrency.Add(float64(W) / float64(B))
			}

			if Q > B {
				// Save internal concurrency while the workers are full.
				if B > 0 {
					c.queuedConcurrency.Add(float64(W) / float64(B))
				}

				// Q > 0 (considering Q <= beta as insignificant, as in high
				// traffic it might be difficult to spot an actual 0) means the
				// system is queued up so just use a y-dot estimation of the
				// rate since workers are operating at full speed.
				c.r = R
				c.b = c.dx / R
				c.k = float64(Q) / R / float64(c.mq)

			} else if W > 0 {
				// The system is either overscaled or in equilibrium. Use the
				// mean between the two rate estimations, in order to bring the
				// lower bound of the rate estimation (obtained though
				// controlling beta) up towards the equilibrium value, given by
				// Little's Theorem.
				// Why not using Little's Theorem right away? To avoid flapping.
				//
				// In our use of the Little's Theorem, we want to know the
				// number of active workers (to know if this is lesser than β,
				// i.e. some are starving). If the workers aren't internally
				// concurrent, this means W is the number of active workers;
				// however for internally concurrent workers this isn't true,
				// we might have a very high W meaning many messages are being
				// processed by the system, while the number of busy workers is
				// still low.
				// In order to have a good estimation of this, we keep an
				// internal concurrency average from samples from when the
				// system is queued up (Q > 0), which should mean that the
				// system is showing its internal concurrency in W / β.
				// We now use that to estimate number of active workers.
				busyWorkers := float64(W) / c.InternalConcurrency()

				yBInv := R / c.dx
				xBInv := 1.0 / busyWorkers
				// Harmonic mean to discard overscaled values
				c.b = 2.0 / (xBInv + yBInv)
				c.r = c.dx / c.b
				c.k = 0

			} else { // X = Y
				// The system has stopped, or it's processing messages too
				// quickly to be able to observe it. Repeat the calculations
				// from above but instead of using the harmonic mean (which
				// would lead b to be 0 always), use the arithmetic mean -- this
				// is, appoint half the workers we'd have if we keep this R.
				// This should eventually bring R closer to reality if it's
				// underestimated (which would be the case for a controller that
				// is started in a system that's already overscaled).
				// We also can't use Little Theorem's, so just keep the computed
				// R.
				c.b = c.dx / R / 2 // Arithmetic mean with X-Y =0.
				c.r = R
				c.k = 0
			}
		}

		if firstIteration {
			// Don't set beta the first iteration since the system hasn't had
			// time to integrate.
			firstIteration = false
			continue
		}

		// c.k is bursty, we allow it to rapidly change.
		c.betaEMA.Add(c.b)

		if c.dryRun {
			continue
		}

		// Set b
		c.plant.SetB() <- c.betaEMA.Value() + c.k

	}

}

func (c *Control) XD() uint {
	if c.dx > 0 {
		return uint(math.Round(c.MuP() / 1000 * c.dx))
	}
	return 0
}

func (c *Control) DX() float64 {
	return c.dx
}

func (c *Control) DY() float64 {
	return c.dy
}

func (c *Control) R() float64 {
	return c.r
}

func (c *Control) B() float64 {
	return c.b
}

func (c *Control) K() float64 {
	return c.k
}

func (c *Control) Beta() float64 {
	return c.betaEMA.Value() + c.k
}

func (c *Control) MuP() float64 {
	mu_p, ok := c.plant.MuP()
	if ok {
		return mu_p
	}

	// Compute from Little's Law
	if c.dx > 0 {
		return float64(c.plant.XmY()) / c.dx
	}

	// No data
	return 0
}

// Internal concurrency of the workers, i.e. how many messages each one
// processes at a time when full. Both samplings are lower bounds of it, so the
// highest is taken, or 1 when there are none.
func (c *Control) InternalConcurrency() float64 {
	return math.Max(1, math.Max(c.internalConcurrency.Value(), c.queuedConcurrency.Value()))
}

// Utilisation of the workers' capacity, from 0 to 1.
func (c *Control) Utilisation() float64 {
	return c.u
}
