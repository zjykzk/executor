package executor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type noop struct{}

func (n noop) Run() {}

type ncounter struct {
	count int32
	sync.WaitGroup
}

func (c *ncounter) Run() {
	//atomic.AddInt32(&c.count, 1)
	fmt.Printf("c:%d\n", atomic.AddInt32(&c.count, 1))
	c.WaitGroup.Done()
}

func TestExecutor(t *testing.T) {
	t.Run("max idle time", testMaxIdleTime)
}

func TestWorkerCount(t *testing.T) {
	e, _ := NewPoolExecutor(2, 2, time.Hour, NewLinkedBlockingQueue())
	assert.Equal(t, 0, e.workerCountOf())
	c := &ncounter{}
	c.Add(1)
	if err := e.Execute(c); err != nil {
		t.Fatal(err)
	}
	c.Wait()
	assert.Equal(t, 1, e.workerCountOf())

	c.Add(1)
	e.Execute(c)
	c.Wait()
	assert.Equal(t, 1, e.workerCountOf())

	c = &ncounter{}
	taskCount := 100000
	c.Add(taskCount)
	for i := 0; i < taskCount; i++ {
		err := e.Execute(c)
		if err != nil {
			t.Fatalf("%d:%v", i, err)
		}
	}
	assert.True(t, e.queue.Size() > 0)
	c.Wait()
	assert.Equal(t, 2, e.workerCountOf())
	assert.Equal(t, int32(taskCount), c.count)
}

func testMaxIdleTime(t *testing.T) {
	e := &GoroutinePoolExecutor{
		corePoolSize: 2,
		maxPoolSize:  4,
		maxIdleTime:  time.Millisecond * 10,
	}

	now := time.Now()
	e.ready = append(e.ready, &workerChan{lastUseTime: now, ch: make(chan Runnable, 1)})

	time.Sleep(time.Millisecond * 20)

	idleWorkers := make([]*workerChan, 0, 10)
	e.cleanIdle(&idleWorkers)
	assert.Equal(t, 0, e.workerCountOf())
}

func TestState(t *testing.T) {
	e := &GoroutinePoolExecutor{}
	e.setState(running)
	assert.Equal(t, running, e.state())
	e.ctl += 100
	assert.Equal(t, running, e.state())
	e.setState(shutdown)
	assert.Equal(t, shutdown, e.state())
}

func TestShutdown(t *testing.T) {
	e, _ := NewPoolExecutor(6, 6, time.Hour, NewLinkedBlockingQueue())
	assert.Equal(t, 0, e.workerCountOf())
	c := &ncounter{}
	taskCount := 10000
	c.Add(taskCount)
	for i := 0; i < taskCount; i++ {
		err := e.Execute(c)
		if err != nil {
			t.Fatalf("%d:%v", i, err)
		}
	}
	assert.True(t, e.queue.Size() > 0)
	e.Shutdown()
	assert.Equal(t, stop, e.state())
	assert.Equal(t, 0, e.queue.Size())
	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, taskCount, int(c.count))
}
