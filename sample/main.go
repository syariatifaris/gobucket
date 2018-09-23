package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/syariatifaris/gobucket"
)

func main() {
	tb := gobucket.NewTaskBucket(&gobucket.BucketConfig{
		LifeSpan:  time.Second,
		MaxBucket: 1024,
		Verbose:   true,
	})
	total := 10
	done := make(chan bool)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(proc int) {
			defer wg.Done()
			data := struct {
				ID   int    `json:"id"`
				Name string `json:"name"`
			}{
				ID:   1,
				Name: "Johnson",
			}
			tb.Fill(context.Background(), gobucket.ImmidiateTask, fmt.Sprintf("process::%d", proc), data, new(sampleExecutor))
		}(i)
	}
	wg.Wait()
	go func() {
		done <- true
		close(done)
	}()
	select {
	case <-done:
		log.Println("all process done")
	}
	time.Sleep(time.Second * 3)
}

type sampleExecutor struct{}

func (se *sampleExecutor) OnExecute(ctx context.Context, id string, data interface{}) error {
	log.Println("ON EXECUTE:", id, "data=", data)
	return nil
}

func (se *sampleExecutor) OnFinish(ctx context.Context, id string, data interface{}) error {
	return nil
}

func (se *sampleExecutor) OnTaskExhausted(ctx context.Context, id string, data interface{}) error {
	//log.Println("on timeout: process id=", id)
	return nil
}

func (se *sampleExecutor) OnExecuteError(ctx context.Context, id string, data interface{}, onExecuteErr error) error {
	//log.Println("on execute error: process id=", id, "previous err=", onExecuteErr.Error())
	return nil
}
