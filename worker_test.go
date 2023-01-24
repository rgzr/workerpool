package workerpool

import (
	"context"
	"testing"
	"time"
)

type MyWork struct {
	progress int
	sleep    time.Duration
	goal     int
}

func (w *MyWork) Work() {
	w.progress += 1
	time.Sleep(w.sleep)
}

func (w *MyWork) Finished() bool {
	return w.progress == w.goal
}

func (w *MyWork) Stop() {}

func myWork1s() *MyWork {
	return &MyWork{goal: 100, sleep: 10 * time.Millisecond}
}

func myWork5s() *MyWork {
	return &MyWork{goal: 100, sleep: 50 * time.Millisecond}
}

func myWork20s() *MyWork {
	return &MyWork{goal: 100, sleep: 200 * time.Millisecond}
}

func TestWorkerBasics(t *testing.T) {
	finishedChan := make(chan *EventWorkFinished, 10)

	slow1 := myWork1s()
	slow2 := myWork1s()

	worker := NewWorker(finishedChan)

	worker.Start(context.Background(), slow1)

	expectFinishedEvent(t, finishedChan, slow1)
	if slow1.progress != 100 {
		t.Fatalf("Slow1 progress is not 100")
	}

	worker.Start(context.Background(), slow1)

	expectFinishedEvent(t, finishedChan, slow1)
	if slow1.progress != 100 {
		t.Fatalf("Slow1 progress is not 100")
	}

	worker.Start(context.Background(), slow2)

	time.Sleep(100 * time.Millisecond)

	worker.Stop()

	if slow2.progress < 10 {
		t.Fatalf("Slow2 progress is less than 10")
	}

	worker.Start(context.Background(), slow2)

	expectFinishedEvent(t, finishedChan, slow2)
	if slow2.progress != 100 {
		t.Fatalf("Slow2 progress is not 100")
	}
}

func TestWorkerStopFinished(t *testing.T) {
	finishedChan := make(chan *EventWorkFinished, 10)

	slow1 := myWork1s()

	worker := NewWorker(finishedChan)

	worker.Start(context.Background(), slow1)

	expectFinishedEvent(t, finishedChan, slow1)
	if slow1.progress != 100 {
		t.Fatalf("Slow1 progress is not 100")
	}

	if worker.Stop() != nil {
		t.Fatalf("Worker should return nil")
	}
}

func expectFinishedEvent(t *testing.T, finishedChan chan *EventWorkFinished, work Work) {
	event := <-finishedChan
	if event.Work != work {
		t.Fatalf("Finished event is not expected work")
	}
}
