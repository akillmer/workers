package workers

import (
	"fmt"
	"log"
	"reflect"
	"sync/atomic"
)

var (
	// ErrAlreadyOpened - the work is already opened
	ErrAlreadyOpened = fmt.Errorf("the work pool is already opened")
	// ErrClosed - the work pool is closed
	ErrClosed = fmt.Errorf("the work pool is closed")
)

// Pool manages all the worker goroutines and dispatched jobs.
type Pool struct {
	pending  uint64
	size     int
	ready    uint32
	close    chan struct{}
	dispatch chan interface{}
	jobFn    func(interface{}) error
}

// NewPool creates a new worker pool that is not yet opened.
func NewPool(size int, job func(interface{}) error) *Pool {
	return &Pool{
		size:  size,
		jobFn: job,
	}
}

// Open the worker pool, returns an error if the pool is already opened.
func (p *Pool) Open() error {
	if p.Ready() {
		return ErrAlreadyOpened
	}
	p.close = make(chan struct{})
	p.dispatch = make(chan interface{}, p.Size())

	// block until the go routine acknowledges
	ack := make(chan struct{})
	for i := 0; i < p.Size(); i++ {
		go p.work(ack)
		<-ack
	}
	close(ack)

	atomic.StoreUint32(&p.ready, 1)
	return nil
}

func (p *Pool) work(ready chan struct{}) {
	ready <- struct{}{}
loop:
	for {
		select {
		case <-p.close:
			break loop
		case data := <-p.dispatch:
			if err := p.jobFn(data); err != nil {
				log.Printf("%s\n\tdata [%v]: %+v",
					err, reflect.TypeOf(data), data)
			}
			atomic.AddUint64(&p.pending, ^uint64(0))
		}
	}
}

// Pending is the number of jobs yet to be dispatched (enqueued).
func (p *Pool) Pending() uint64 {
	return atomic.LoadUint64(&p.pending)
}

// Wait blocks until all pending jobs are dispatched.
func (p *Pool) Wait() {
	for p.Pending() > 0 {
	}
}

// Size is the number of workers inside this pool.
func (p *Pool) Size() int {
	return p.size
}

// Ready indicates whether the pool can do work.
func (p *Pool) Ready() bool {
	return atomic.LoadUint32(&p.ready) == 1
}

// Close prevents any new work from being sent to the work pool. Any jobs
// that are being processed are finished. The value returned is the number
// of enqueued jobs that were dropped.
func (p *Pool) Close() int {
	atomic.StoreUint32(&p.ready, 0) // stop taking new work

	// stop all workers
	for i := 0; i < p.Size(); i++ {
		p.close <- struct{}{}
	}
	close(p.close)

	// drain the dispatch channel
	canceled := int(p.Pending())
	for i := 0; i < canceled; i++ {
		<-p.dispatch
	}
	close(p.dispatch)

	atomic.StoreUint64(&p.pending, 0)
	return canceled
}

// Send queues up the job data for the worker pool. This is non-blocking
// and will only return an error if the pool is closed.
func (p *Pool) Send(job interface{}) error {
	if p.Ready() {
		go func(pool *Pool, data *interface{}) {
			select {
			case pool.dispatch <- *data:
				break
			}
		}(p, &job)
		atomic.AddUint64(&p.pending, 1)
		return nil
	}
	return ErrClosed
}
