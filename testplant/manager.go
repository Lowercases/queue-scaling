package testplant

import (
	"math"
	"sync"
	"time"
)

type Manager struct {
	setB       chan float64
	newMessage chan struct{}

	// Communication with workers
	processed chan uint

	// State
	// Workers -- they need concurrency
	workers      []*Worker // Active workers
	workersMutex sync.Mutex

	queue *Queue // Pending work for workers
	x, y  uint
	mu_p  float64
	// Beta needs a concurrency primitive
	beta      uint
	betaMutex sync.Mutex

	// Derivative keep, also needing a concurrency primitive
	dx, dy     uint
	dTimestamp time.Time
	dMutex     sync.Mutex

	// Settings
	mu_p0, sigma_p0 uint
	unit            time.Duration
}

func NewManager(mu_p0, sigma_p0 uint, unit time.Duration) *Manager {
	m := &Manager{
		setB:       make(chan float64),
		newMessage: make(chan struct{}),

		processed: make(chan uint),

		workers:  []*Worker{},
		queue:    NewQueue(),
		mu_p0:    mu_p0,
		sigma_p0: sigma_p0,
		unit:     unit,

		dTimestamp: time.Now(),
	}

	go m.run()

	return m
}

func (m *Manager) run() {
	for {
		select {
		case b := <-m.setB:
			m.setBeta(bToBeta(b))

		case p, ok := <-m.newMessage:
			if !ok {
				close(m.queue.Send)
				return
			}
			m.queue.Send <- p
			m.x++

			m.dMutex.Lock()
			m.dx++
			m.dMutex.Unlock()

		case d := <-m.processed:
			// Update median and total
			nmu_p := m.mu_p

			nmu_p *= float64(m.y)
			nmu_p += float64(d)
			m.y++
			nmu_p /= float64(m.y)

			// Changed atomically so to avoid concurrency issues
			m.mu_p = nmu_p

			m.dMutex.Lock()
			m.dy++
			m.dMutex.Unlock()

		}
	}
}

func (m *Manager) SetB() chan float64 {
	return m.setB
}

func (m *Manager) Message() chan struct{} {
	return m.newMessage
}

func (m *Manager) Add(messages uint) {
	m.queue.Add(messages)
}

func (m *Manager) setBeta(beta uint) {
	m.betaMutex.Lock()

	m.beta = beta

	m.betaMutex.Unlock()

	go func() {
		m.workersMutex.Lock()
		for len(m.workers) > int(beta) {
			go m.workers[len(m.workers)-1].Kill()
			m.workers = m.workers[:len(m.workers)-1]
		}
		for len(m.workers) < int(beta) {
			m.workers = append(m.workers, NewWorker(m.queue, m.processed, m.mu_p0, m.sigma_p0, m.unit))
		}
		m.workersMutex.Unlock()
	}()

}

func (m *Manager) DXY(unit time.Duration) (dx, dy float64) {
	m.dMutex.Lock()
	now := time.Now()
	elapsed := now.Sub(m.dTimestamp)
	m.dTimestamp = now

	elapsed /= unit
	dx, dy = float64(m.dx)/float64(elapsed), float64(m.dy)/float64(elapsed)

	m.dx, m.dy = 0, 0
	m.dMutex.Unlock()

	return
}

func (m *Manager) X() uint {
	return m.x
}

func (m *Manager) Y() uint {
	return m.y
}

func (m *Manager) XmY() uint {
	return m.x - m.y
}

func (m *Manager) MuP() (float64, bool) {
	return m.mu_p, true
}

func (m *Manager) Beta() uint {
	return uint(len(m.workers))
}

func (m *Manager) Q() uint {
	return m.queue.pending
}

func bToBeta(b float64) uint {
	// Round either way
	return uint(math.Round(b))
}
