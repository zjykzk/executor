package executor

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	countBits = 29
	capacity  = (1 << countBits) - 1

	running uint32 = (iota - 1) << 29
	shutdown
	stop
)

// Executor executes the submitted tasks
type Executor interface {
	Execute(r Runnable) error
}

type workerChan struct {
	lastUseTime time.Time
	ch          chan Runnable
}

// GoroutinePoolExecutor executor with pooled goroutie
type GoroutinePoolExecutor struct {
	corePoolSize, maxPoolSize, workerCount uint32

	workerChanPool sync.Pool
	queue          BlockQueue
	queueCh        chan Runnable

	locker sync.RWMutex
	ready  []*workerChan

	wg          sync.WaitGroup
	stopCh      chan struct{}
	maxIdleTime time.Duration
}

var workerChanCap = func() int {
	// Use blocking workerChan if GOMAXPROCS=1.
	// This immediately switches Serve to WorkerFunc, which results
	// in higher performance (under go1.5 at least).
	if runtime.GOMAXPROCS(0) == 1 {
		return 0
	}

	// Use non-blocking workerChan if GOMAXPROCS>1,
	// since otherwise the Serve caller (Acceptor) may lag accepting
	// new connections if WorkerFunc is CPU-bound.
	return 1
}()

// NewPoolExecutor creates GoroutinePoolExecutor
func NewPoolExecutor(
	corePoolSize, maxPoolSize uint32,
	keepLiveTime time.Duration,
	queue BlockQueue,
) (*GoroutinePoolExecutor, error) {
	if corePoolSize > maxPoolSize {
		return nil, errors.New("corePoolSize bigger maximumPoolSize")
	}

	if queue == nil {
		return nil, errors.New("empgy queue")
	}

	ex := &GoroutinePoolExecutor{
		corePoolSize: corePoolSize,
		maxPoolSize:  maxPoolSize,
		maxIdleTime:  keepLiveTime,
		queue:        queue,
		queueCh:      make(chan Runnable, 32),
		ready:        make([]*workerChan, 0, 32),
		stopCh:       make(chan struct{}),
		workerChanPool: sync.Pool{New: func() interface{} {
			return &workerChan{ch: make(chan Runnable, workerChanCap)}
		}},
	}

	ex.start()

	return ex, nil
}

// Execute run one task
func (e *GoroutinePoolExecutor) Execute(r Runnable) error {
	if r == nil {
		return errBadRunnable
	}

	//if e.state() != running {
	//return errNotRunning
	//}

	createWorker := false
	workerCount := atomic.LoadUint32(&e.workerCount)
	switch {
	case workerCount < e.corePoolSize:
		createWorker = true
	case workerCount < e.maxPoolSize && e.queue.IsFull():
		createWorker = true
	}

	if !createWorker {
		if e.queue.IsFull() {
			return errQueueIsFull
		}

		e.queue.Put(r)
		return nil
	}

	wch := e.getWorkerCh()
	wch.ch <- r
	go func() {
		e.startWorker(wch)
		e.workerChanPool.Put(wch)
	}()

	return nil
}

// Shutdown shutdown the executor
func (e *GoroutinePoolExecutor) Shutdown() {
	e.setState(shutdown)
	close(e.stopCh)
	e.queue.Put(nil)
	e.wg.Wait()
	e.setState(stop)
}

func (e *GoroutinePoolExecutor) readyCount() (c uint32) {
	e.locker.RLock()
	c = uint32(len(e.ready))
	e.locker.RUnlock()
	return
}

func (e *GoroutinePoolExecutor) start() {
	e.setState(running)
	e.wgWrap(func() {
		idleWorkers := make([]*workerChan, 0, 128)
		select {
		case <-e.stopCh:
			return
		default:
			e.cleanIdle(&idleWorkers)
		}
	})

	e.wgWrap(e.startFeedFromQueue)
}

func (e *GoroutinePoolExecutor) cleanIdle(idleWorkers *[]*workerChan) {
	now := time.Now()
	e.locker.Lock()
	ready := e.ready
	n, l, maxIdleTime := 0, len(ready), e.maxIdleTime
	for n < l && now.Sub(e.ready[n].lastUseTime) > maxIdleTime {
		n++
	}

	if n > 0 {
		*idleWorkers = append((*idleWorkers)[:0], ready[:n]...)
		m := copy(ready, ready[n:])
		for i := m; i < l; i++ {
			ready[i] = nil
		}
		e.ready = ready[:m]
	}
	e.locker.Unlock()

	tmp := *idleWorkers
	for i, w := range tmp {
		w.ch <- nil
		tmp[i] = nil
	}
}

func (e *GoroutinePoolExecutor) getWorkerCh() *workerChan {
	e.locker.Lock()
	ready := e.ready
	n := len(ready) - 1
	if n >= 0 {
		w := ready[n]
		ready[n] = nil
		e.ready = ready[:n]
		e.locker.Unlock()
		return w
	}
	e.workerCount++
	e.locker.Unlock()

	w := e.workerChanPool.Get()
	if w != nil {
		return w.(*workerChan)
	}
	return &workerChan{ch: make(chan Runnable, workerChanCap)}
}

func (e *GoroutinePoolExecutor) startWorker(wch *workerChan) {
	var (
		r         Runnable
		fromQueue bool
	)

	for {
		select {
		case r = <-e.queueCh:
			fromQueue = true
		case r = <-wch.ch:
		}

		if r == nil {
			break
		}

		r.Run()

		wch.lastUseTime = time.Now()
		if !fromQueue {
			e.locker.Lock()
			e.ready = append(e.ready, wch)
			e.locker.Unlock()
		}
	}

	atomic.AddUint32(&e.workerCount, ^uint32(0))
}

func (e *GoroutinePoolExecutor) startFeedFromQueue() {
	for {
		r, err := e.queue.Take()
		if err != nil || r == nil {
			return
		}

		e.queueCh <- r
	}
}

func (e *GoroutinePoolExecutor) workerCountOf() int {
	c := atomic.LoadUint32(&e.workerCount)
	return int(c & capacity)
}

func (e *GoroutinePoolExecutor) state() uint32 {
	c := atomic.LoadUint32(&e.workerCount)
	return c &^ countBits
}

func (e *GoroutinePoolExecutor) setState(s uint32) {
	//for {
	//c := atomic.LoadUint32(&e.workerCount)
	//nc := c&capacity | s
	//if atomic.CompareAndSwapUint32(&e.workerCount, c, nc) {
	//break
	//}
	//}
}

func (e *GoroutinePoolExecutor) wgWrap(f func()) {
	e.wg.Add(1)
	go func() {
		f()
		e.wg.Done()
	}()
}
