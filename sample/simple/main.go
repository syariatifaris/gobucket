package simple

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/syariatifaris/gobucket"
)

func recoverPanic(tb gobucket.TaskBucket) {
	if r := recover(); r != nil {
		tb.Rescue(context.Background())
		fmt.Println("recover from:", r)
	}
}

func main() {
	scheduleTask()
}

func scheduleTask() {
	tb := gobucket.NewTaskBucket(&gobucket.BucketConfig{
		LifeSpan:  time.Second * 2,
		MaxBucket: 1024,
		Verbose:   true,
		RunAfter:  time.Second,
	}, new(sampleExecutor))
	defer recoverPanic(tb)
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
			//cfg.RunAfter will bee ignored
			tb.Fill(context.Background(), gobucket.ImmidiateTask, fmt.Sprintf("process::%d", proc), data)
		}(i)
	}
	wg.Wait()
	go func() {
		done <- true
		close(done)
	}()
	select {
	case <-done:
		log.Println("all process has been scheduled")
	}
	time.Sleep(time.Second * 3)
}

type sampleExecutor struct{}

func doRequest() ([]byte, error) {
	rs, err := http.Get("https://api.github.com/users/syariatifaris/repos")
	// Process response
	if err != nil {
		return nil, err
	}
	defer rs.Body.Close()
	bodyBytes, err := ioutil.ReadAll(rs.Body)
	if err != nil {
		return nil, err
	}
	return bodyBytes, err
}

func (se *sampleExecutor) OnExecute(ctx context.Context, id string, data interface{}) error {
	log.Println("ON EXECUTE: response done")
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

func (se *sampleExecutor) OnPanic(ctx context.Context, id string, data interface{}) error {
	log.Println("on panic: process id=", id)
	return nil
}
