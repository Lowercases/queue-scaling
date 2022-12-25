package testplant

type Queue struct {
	// Since messages are empty, we can just use an integer for the pending ones
	pending uint

	Send, Recv chan struct{}
}

func NewQueue() *Queue {
	q := &Queue{
		Send: make(chan struct{}),
		Recv: make(chan struct{}),
	}

	// Consumer/producer/waiter
	// FIXME to run.
	go func() {
		for {
			// If there are values in queue, accept reads and writes
			if q.pending > 0 {
				select {
				case _, ok := <-q.Send:
					if !ok {
						return // Finish
					}
					q.pending++
				case q.Recv <- *new(struct{}):
					q.pending--
				}
			} else {
				// Wait until we've received
				_, ok := <-q.Send
				if !ok {
					return
				}
				q.pending++
			}
		}
	}()

	return q

}

func (q *Queue) Add(messages uint) {
	q.pending += messages
}
