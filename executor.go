package executor

import (
	"sync"
	"time"
)

// Executor executes the submitted tasks
type Executor interface {
	Execute(r Runnable)
}

type workerChan struct {
	lastUseTime time.Time
	ch          chan Runnable
}

// GoroutinePoolExecutor executor with pooled goroutie
type GoroutinePoolExecutor struct {
	corePoolSize, maximumPoolSize int32
	keeyLiveTime                  time.Duration

	ready []*workerChan

	stopCh chan struct{}

	workerChanPool sync.Pool
}
