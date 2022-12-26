package control

import (
	"math"
	"time"
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

	// Beta integral and y estimation
	y                     float64
	betaIntegral          float64
	betaIntegralTimestamp time.Time
}

func NewControl(plant Manager, controlPeriod, maxQueueTime uint, unit time.Duration) *Control {
	return &Control{
		plant:                 plant,
		t:                     controlPeriod,
		mq:                    maxQueueTime,
		unit:                  unit,
		betaIntegralTimestamp: time.Now(),
	}
}

func (c *Control) SetDryRun() {
	c.dryRun = true
}

func (c *Control) Start() {
	go c.run()
}

func (c *Control) run() {
	firstIteration := true

	for {
		time.Sleep(time.Duration(c.t) * c.unit)

		c.dx, c.dy = c.plant.DXY(c.unit)
		XmY := c.plant.XmY()
		B := c.plant.Beta()
		Q := c.plant.Q()

		c.y += c.dy * float64(c.t)
		c.updateBetaIntegral(B)

		if c.y < 1 || c.betaIntegral < 1 {
			// Exclusive Little's theorem estimation of the rate since the
			// system hasn't started yet. The system will necessarily overshoot,
			// since we've got no point of reference.
			c.b = float64(XmY)

		} else { // X >= Y > 0
			// The system is operating with a non-zero input and output rate.
			// Compute the output throughput R, using the highest between
			// instant throughput (y-dot/beta) and historic (y/B), since the
			// latter is less exact but good when beta is close to zero, when
			// the workers are very likely to be starved.
			R := c.y / c.betaIntegral
			if B > 0 {
				RI := c.dy / float64(B)
				if RI > R {
					R = RI
				}
			}

			if Q > 0 {
				// Q > 0 means the system is queued up so just use a y-dot
				// estimation of the rate since workers are operating at full
				// speed.
				c.r = R
				c.b = c.dx / R
				c.k = float64(Q) / R / float64(c.mq)

			} else if XmY > 0 {
				// The system is either overscaled or in equilibrium. Use the
				// mean between the two rate estimations, in order to bring the
				// lower bound of the rate estimation (obtained though
				// controlling beta) upwards towards the equilibrium value,
				// given by Little's Theorem.
				// Why not using Little's Theorem right away? To avoid flapping.
				yBInv := R / c.dx
				xBInv := 1.0 / float64(XmY)
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

		if c.dryRun {
			continue
		}

		if firstIteration {
			// Don't set beta the first iteration since the system hasn't had
			// time to integrate.
			firstIteration = false
			continue
		}

		// Set b
		c.plant.SetB() <- c.b + c.k

	}

}

func (c *Control) updateBetaIntegral(beta uint) {
	now := time.Now()
	elapsed := now.Sub(c.betaIntegralTimestamp)

	c.betaIntegral += elapsed.Seconds() * float64(time.Second/c.unit) * float64(beta)
	c.betaIntegralTimestamp = now

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
