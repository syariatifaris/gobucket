package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/syariatifaris/arkeus/core/uuid"

	"github.com/syariatifaris/gobucket"
)

var (
	port  = flag.String("port", "6666", "default tcp server port")
	debug = flag.Bool("debug", false, "default debug mode")
)

func init() {
	flag.Parse()
}

func main() {
	tb := gobucket.NewTaskBucket(&gobucket.BucketConfig{
		LifeSpan:  time.Second * 2,
		MaxBucket: 1,
		Verbose:   true,
		RunAfter:  time.Second,
	}, new(sampleExecutor))

	group := make(map[string]gobucket.TaskBucket, 0)
	group["sample"] = tb
	peers := []string{
		"127.0.0.1:6666", //will be excluded
		"127.0.0.1:6667",
		"127.0.0.1:6668",
	}
	peers = exclude(*port, peers)
	bg := gobucket.NewTaskBucketGroup(group, peers, *port, *debug)
	log.Println("start serving..")
	go func() {
		for {
			time.Sleep(time.Second / 10)
			uid, _ := uuid.GetV4UUID()
			err := bg.Fill(context.Background(), "sample", uid, "its a data")
			if err != nil {
				log.Println("main:", err.Error())
				continue
			}
		}
	}()
	bg.StartWork()
}

func exclude(port string, peers []string) []string {
	var np []string
	for _, p := range peers {
		if !strings.Contains(p, port) {
			np = append(np, p)
		}
	}
	return np
}

type sampleExecutor struct{}

func (se *sampleExecutor) OnExecute(ctx context.Context, id string, data interface{}) error {
	time.Sleep(time.Second)
	log.Println("ON EXECUTE: response done", data)
	return nil
}

func (se *sampleExecutor) OnFinish(ctx context.Context, id string, data interface{}) error {
	return nil
}

func (se *sampleExecutor) OnTaskExhausted(ctx context.Context, id string, data interface{}) error {
	return nil
}

func (se *sampleExecutor) OnExecuteError(ctx context.Context, id string, data interface{}, onExecuteErr error) error {
	return nil
}

func (se *sampleExecutor) OnPanic(ctx context.Context, id string, data interface{}) error {
	log.Println("on panic: process id=", id)
	return nil
}
