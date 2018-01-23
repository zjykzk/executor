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

	running int32 = (iota - 1) << 29
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
	corePoolSize, maxPoolSize, ctl int32

	workerChanPool sync.Pool
	queue          BlockQueue

	locker       sync.RWMutex
	hasWorker    *sync.Cond
	ready        []*workerChan
	pendingCount sync.WaitGroup

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
	corePoolSize, maxPoolSize int32,
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
		ready:        make([]*workerChan, 0, 32),
		stopCh:       make(chan struct{}),
		workerChanPool: sync.Pool{New: func() interface{} {
			return &workerChan{ch: make(chan Runnable, workerChanCap)}
		}},
	}

	ex.hasWorker = sync.NewCond(&ex.locker)

	ex.start()

	return ex, nil
}

// Execute run one task
func (e *GoroutinePoolExecutor) Execute(r Runnable) error {
	if r == nil {
		return errBadRunnable
	}

	if e.state() != running {
		return errNotRunning
	}

	createWorker := false
	workerCount := int32(e.workerCountOf())
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

		e.pendingCount.Add(1)
		e.queue.Put(r)
		return nil
	}

	e.pendingCount.Add(1)
	wch := e.getWorkerCh()
	wch.ch <- r
	e.wgWrap(func() {
		e.startWorker(wch)
		e.workerChanPool.Put(wch)
	})

	return nil
}

// Shutdown shutdown the executor
func (e *GoroutinePoolExecutor) Shutdown() {
	e.setState(shutdown)
	close(e.stopCh)

	e.queue.Put(nil)
	e.pendingCount.Wait()
	e.shutdownWorker()

	e.wg.Wait()
	e.setState(stop)
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

	e.wgWrap(func() {
		e.startFeedFromQueue()
	})
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
	e.ctl++
	e.locker.Unlock()

	w := e.workerChanPool.Get()
	if w != nil {
		return w.(*workerChan)
	}
	return &workerChan{ch: make(chan Runnable, workerChanCap)}
}

func (e *GoroutinePoolExecutor) startWorker(wch *workerChan) {
	for r := range wch.ch {
		if r == nil {
			break
		}

		r.Run()

		e.pendingCount.Done()

		wch.lastUseTime = time.Now()
		e.locker.Lock()
		e.ready = append(e.ready, wch)
		e.locker.Unlock()
		e.hasWorker.Signal()
	}

	atomic.AddInt32(&e.ctl, -1)
}

func (e *GoroutinePoolExecutor) shutdownWorker() {
	e.locker.RLock()
	for _, w := range e.ready {
		close(w.ch)
	}
	e.locker.RUnlock()
}

func (e *GoroutinePoolExecutor) startFeedFromQueue() {
	for {
		r, err := e.queue.Take()
		if err != nil || r == nil {
			break
		}

		e.hasWorker.L.Lock()
		for len(e.ready) == 0 {
			e.hasWorker.Wait()
		}
		n := len(e.ready) - 1
		wch := e.ready[n]
		e.ready = e.ready[:n]
		e.hasWorker.L.Unlock()
		wch.ch <- r
	}
}

func (e *GoroutinePoolExecutor) workerCountOf() int {
	c := atomic.LoadInt32(&e.ctl)
	return int(c & capacity)
}

func (e *GoroutinePoolExecutor) state() int32 {
	c := atomic.LoadInt32(&e.ctl)
	return c &^ capacity
}

func (e *GoroutinePoolExecutor) setState(s int32) {
	for {
		c := atomic.LoadInt32(&e.ctl)
		nc := c&capacity | s
		if atomic.CompareAndSwapInt32(&e.ctl, c, nc) {
			break
		}
	}
}

func (e *GoroutinePoolExecutor) wgWrap(f func()) {
	e.wg.Add(1)
	go func() {
		f()
		e.wg.Done()
	}()
}
