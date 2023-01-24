package workerpool

import (
	"context"
)

type worker struct {
	ctx          context.Context
	cancelFunc   context.CancelFunc
	work         Work
	finishedChan chan<- *EventWorkFinished
	stopChan     chan Work
}

type Work interface {
	Work()
	Finished() bool
	Stop()
}

type EventWorkFinished struct {
	Worker *worker
	Work   Work
}

func NewWorker(finishedChan chan<- *EventWorkFinished) *worker {
	return &worker{
		finishedChan: finishedChan,
		stopChan:     make(chan Work, 1),
	}
}

func (w *worker) Start(ctx context.Context, work Work) {
	w.work = work
	w.ctx, w.cancelFunc = context.WithCancel(ctx)

	go w.loop()
}

func (w *worker) Stop() Work {
	if w.work != nil {
		w.cancelFunc()
		return <-w.stopChan
	}
	return nil
}

func (w *worker) loop() {
	finished := false
WorkLoop:
	for {
		select {
		case <-w.ctx.Done():
			break WorkLoop
		default:
			if w.work.Finished() {
				finished = true
				break WorkLoop
			}
			w.work.Work()
		}
	}

	w.work.Stop()

	work := w.work

	w.ctx = nil
	w.cancelFunc = nil
	w.work = nil

	if finished {
		w.finishedChan <- &EventWorkFinished{Worker: w, Work: work}
	} else {
		w.stopChan <- work
	}
}
