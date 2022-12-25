package testplant

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

type Generator struct {
	logMu, logSigma float64
	unit            time.Duration
	plant           Plant
	killed          bool
	sync.Mutex      // For kill
}

func NewGenerator(plant Plant, logMu, logSigma float64, unit time.Duration) *Generator {
	return &Generator{
		logMu:    logMu,
		logSigma: logSigma,
		unit:     unit,
		plant:    plant,
		killed:   false,
	}
}

func (g *Generator) Start() {
	go g.run()
}

func (g *Generator) run() {
	for {
		LogSleep(g.logMu, g.logSigma, g.unit)

		g.Lock()
		if g.killed {
			g.Unlock()
			break
		}
		g.plant.Message() <- *new(struct{})
		g.Unlock()
	}
}

func (g *Generator) Kill() {
	g.Lock()
	g.killed = true
	g.Unlock()
}

func (g *Generator) IncreaseLogMu(delta float64) {
	g.logMu += delta
}

func (g *Generator) Burst(relMu, size uint) {
	go g.burst(g.logMu-float64(relMu), size)
}

func (g *Generator) burst(burstLogMu float64, size uint) {
	var i uint
	for i = 0; i < size; i++ {
		LogSleep(burstLogMu, g.logSigma, g.unit)
		g.plant.Message() <- *new(struct{})
	}
}

// Convenience function for sleeping
func LogSleep(logMu, logSigma float64, unit time.Duration) {
	r := math.Exp(rand.NormFloat64()*logSigma + logMu)
	// Increase precision to nanoseconds
	r *= float64(unit / time.Nanosecond)
	sl := time.Duration(r)
	time.Sleep(sl)
}
