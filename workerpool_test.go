package workerpool

import (
	"log"
	"testing"
)

func TestWorkerpool(t *testing.T) {
	workerpool := NewWorkerPool(&Config{
		MaxWorkers: 5,
		StateFunc: func(wp *WorkerPool, state State) {
			log.Printf("State is %v", state)
		},
	})
	workerpool.Start()
	workerpool.Stop()
}
