package workerpool

import (
	"context"
	"log"
	"sync"
)

type State uint8

const (
	StateStopped State = iota
	StateStarting
	StateStarted
	StateStopping
)

type StateFunc func(*WorkerPool, State)

type WorkerPool struct {
	*sync.Mutex
	ctx          context.Context
	cancelFunc   context.CancelFunc
	stopChan     chan struct{}
	state        State
	stateFunc    StateFunc
	maxWorkers   int
	working      []*worker
	idle         []*worker
	workQueue    []Work
	finishedChan chan *EventWorkFinished
	finished     int
}

type Config struct {
	MaxWorkers int
	StateFunc  StateFunc
}

type Info struct {
	MaxWorkers int
	Working    int
	Idle       int
	Queued     int
	Finished   int
}

func NewWorkerPool(config *Config) *WorkerPool {
	wp := &WorkerPool{
		Mutex:        &sync.Mutex{},
		stopChan:     make(chan struct{}, 1),
		state:        StateStopped,
		working:      []*worker{},
		idle:         []*worker{},
		workQueue:    []Work{},
		finishedChan: make(chan *EventWorkFinished, 1000),
	}
	wp.updateConfig(config)
	return wp
}

func (wp *WorkerPool) AddWorks(work ...Work) {
	wp.Lock()
	wp.workQueue = append(wp.workQueue, work...)
	wp.assignWork()
	wp.Unlock()
}

func (wp *WorkerPool) updateConfig(config *Config) {
	if config.MaxWorkers != wp.maxWorkers {
		wp.maxWorkers = config.MaxWorkers
		if wp.state == StateStarted {
			wp.updateWorkers()
		}
	}
	wp.stateFunc = config.StateFunc
}

func (wp *WorkerPool) updateWorkers() {
	wp.removeExcessWorkers()
	wp.createMissingWorkers()
	wp.assignWork()
}

func (wp *WorkerPool) finishedWorker(worker *worker) {
	if wp.pendingWorks() > 0 {
		work := wp.dequeWork()
		worker.Start(wp.ctx, work)
	} else {
		wp.returnWorkerToIdle(worker)
	}
}

func (wp *WorkerPool) returnWorkerToIdle(worker *worker) {
	for i, w := range wp.working {
		if w == worker {
			wp.working = append(wp.working[:i], wp.working[i+1:]...)
			wp.idle = append(wp.idle, worker)
		}
	}
}

func (wp *WorkerPool) finishedWork(work Work) {
	log.Printf("Finished %v", work)
	wp.finished++
}

func (wp *WorkerPool) assignWork() {
	doableWorks := wp.dequeDoableWorks()
	idleWorkersToWork := wp.idle[len(doableWorks):]
	for i, work := range doableWorks {
		idleWorkersToWork[i].Start(wp.ctx, work)
	}
	wp.working = append(wp.working, idleWorkersToWork...)
	wp.removeIdleWorkers(len(doableWorks))
}

func (wp *WorkerPool) createMissingWorkers() {
	missing := wp.missingWorkers()
	if missing <= 0 {
		return
	}
	wp.addIdleWorkers(missing)
}

func (wp *WorkerPool) removeExcessWorkers() {
	excess := wp.excessWorkers()
	if excess <= 0 {
		return
	}
	if excess < wp.idleWorkers() {
		wp.removeIdleWorkers(excess)
	} else {
		wp.stopWorkers(excess - wp.idleWorkers())
		wp.removeIdleWorkers(wp.idleWorkers())
	}
}

func (wp *WorkerPool) stopWorkers(n int) {
	stopWorkers := wp.working[len(wp.working)-n:]
	stoppedWorks := []Work{}
	stoppedWorksChan := make(chan Work, len(stopWorkers))
	for _, stopWorker := range stopWorkers {
		go func(worker *worker) {
			stoppedWorksChan <- worker.Stop()
		}(stopWorker)
	}
	for i := 0; i < len(stopWorkers); i++ {
		stoppedWork := <-stoppedWorksChan
		if stoppedWork != nil {
			stoppedWorks = append(stoppedWorks, stoppedWork)
		}
	}
	wp.returnWorksToQueue(stoppedWorks)
	wp.idle = append(wp.idle, stopWorkers...)
	wp.working = wp.working[:len(wp.working)-n]
}

func (wp *WorkerPool) removeIdleWorkers(n int) {
	if n > wp.idleWorkers() {
		n = wp.idleWorkers()
	}
	wp.idle = wp.idle[:wp.idleWorkers()-n]
}

func (wp *WorkerPool) returnWorksToQueue(works []Work) {
	wp.workQueue = append(works, wp.workQueue...)
}

func (wp *WorkerPool) addIdleWorkers(n int) {
	for i := 0; i < n; i++ {
		wp.idle = append(wp.idle, NewWorker(wp.finishedChan))
	}
}

func (wp *WorkerPool) dequeDoableWorks() []Work {
	doable := wp.doableWorks()
	doableWorks := wp.workQueue[:doable]
	wp.workQueue = wp.workQueue[doable:]
	return doableWorks
}

func (wp *WorkerPool) dequeWork() Work {
	work := wp.workQueue[0]
	wp.workQueue = wp.workQueue[1:]
	return work
}

func (wp *WorkerPool) workingWorkers() int {
	return len(wp.working)
}

func (wp *WorkerPool) idleWorkers() int {
	return len(wp.idle)
}

func (wp *WorkerPool) pendingWorks() int {
	return len(wp.workQueue)
}

func (wp *WorkerPool) doableWorks() int {
	if wp.pendingWorks() < wp.idleWorkers() {
		return wp.pendingWorks()
	}
	return wp.idleWorkers()
}

func (wp *WorkerPool) excessWorkers() int {
	excess := wp.workingWorkers() + wp.idleWorkers() - wp.maxWorkers
	if excess < 0 {
		return 0
	}
	return excess
}

func (wp *WorkerPool) missingWorkers() int {
	missing := wp.maxWorkers - wp.workingWorkers() - wp.idleWorkers()
	if missing < 0 {
		return 0
	}
	return missing
}

func (wp *WorkerPool) setState(state State) {
	if state != wp.state {
		wp.state = state
		if wp.stateFunc != nil {
			wp.stateFunc(wp, state)
		}
	}
}

// Start starts the worker pool and its workers, blocks until started.
// If already started, returns immediately.
func (wp *WorkerPool) Start() {
	wp.Lock()
	defer wp.Unlock()

	if wp.state == StateStarted {
		return
	}

	wp.setState(StateStarting)

	wp.ctx, wp.cancelFunc = context.WithCancel(context.Background())

	wp.updateWorkers()

	go wp.listen()

	wp.setState(StateStarted)
}

func (wp *WorkerPool) listen() {
ListenLoop:
	for {
		select {
		case finished := <-wp.finishedChan:
			wp.Lock()
			wp.finishedWork(finished.Work)
			wp.finishedWorker(finished.Worker)
			wp.Unlock()
		case <-wp.ctx.Done():
			break ListenLoop
		}
	}

	wp.stopWorkers(wp.workingWorkers())

EmptyFinishedLoop:
	for {
		select {
		case finished := <-wp.finishedChan:
			wp.finishedWork(finished.Work)
		default:
			break EmptyFinishedLoop
		}
	}

	wp.stopChan <- struct{}{}
}

// Stop stops the worker pool and its workers, blocks until stopped.
// If already stopped returns immediately.
func (wp *WorkerPool) Stop() {
	wp.Lock()
	defer wp.Unlock()

	if wp.state == StateStopped {
		return
	}

	wp.setState(StateStopping)

	wp.cancelFunc()

	<-wp.stopChan

	wp.setState(StateStopped)
}

func (wp *WorkerPool) SetConfig(config *Config) {
	wp.Lock()
	wp.updateConfig(config)
	wp.Unlock()
}

func (wp *WorkerPool) GetInfo() *Info {
	wp.Lock()
	defer wp.Unlock()

	return &Info{
		MaxWorkers: wp.maxWorkers,
		Working:    wp.workingWorkers(),
		Idle:       wp.idleWorkers(),
		Queued:     wp.pendingWorks(),
		Finished:   wp.finished,
	}
}
