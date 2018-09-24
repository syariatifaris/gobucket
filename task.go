package gobucket

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

type taskErrType error

var (
	errOnExecute       taskErrType = errors.New("error on main logic execution")
	errOnExecuteErr    taskErrType = errors.New("error on backup logic execution")
	errOnFinish        taskErrType = errors.New("error on on finish logic execution")
	errOnTaskExhausted taskErrType = errors.New("error on task exhausted logic execution")
)

const (
	ImmidiateTask TaskType = "ImmidiateTask"
	TimeBombTask  TaskType = "TimeBomb"
)

type TaskType string

type BucketConfig struct {
	LifeSpan  time.Duration
	RunAfter  time.Duration
	MaxBucket int
	Verbose   bool
}

type task interface {
	run(ctx context.Context, e Executor)
	drain(ctx context.Context, quitting bool) error
	rescue(ctx context.Context) error
}

type baseTask struct {
	verbose     bool
	tb          TaskBucket
	signalQuit  chan bool
	isQuit      bool
	taskType    TaskType
	signalPanic chan bool
}

type taskImpl struct {
	*bucket
	*baseTask
	runAfter time.Duration
}

type bucket struct {
	id           string
	lifeSpan     time.Duration
	taskErr      error
	onExecuteErr error
	data         interface{}
}

func newTask(taskType TaskType, id string, cfg *BucketConfig, data interface{}, tb TaskBucket) task {
	return &taskImpl{
		bucket: &bucket{
			id:       id,
			lifeSpan: cfg.LifeSpan,
			data:     data,
		},
		baseTask: &baseTask{
			verbose:     cfg.Verbose,
			tb:          tb,
			signalQuit:  make(chan bool),
			signalPanic: make(chan bool),
			taskType:    taskType,
		},
		runAfter: cfg.RunAfter,
	}
}

func (t *taskImpl) run(ctx context.Context, e Executor) {
	rctx, cancel := context.WithTimeout(ctx, t.lifeSpan)
	defer cancel()
	var shouldCleanup bool
	finished := make(chan bool)
	go func() {
		if t.baseTask.taskType == TimeBombTask {
			//wait first for timebomb
			t.log(t.id, "wait for ", t.runAfter.Seconds(), " second")
			time.Sleep(t.runAfter)
		}
		if !t.baseTask.isQuit {
			t.onExecuteErr = e.OnExecute(rctx, t.id, t.data)
		}
		finished <- true
		close(finished)
	}()
	select {
	case <-rctx.Done():
		t.log(t.id, "context deadline exceeded after ", t.lifeSpan.Seconds(), " second")
		t.taskErr = errors.New("context deadline exceeded")
		if err := e.OnTaskExhausted(rctx, t.id, t.data); err != nil {
			t.taskErr = t.err(errOnTaskExhausted, err)
		}
		shouldCleanup = true
	case <-finished:
		t.log(t.id, "finished executed")
		if t.onExecuteErr != nil {
			t.log(t.id, "executed with error=", t.onExecuteErr.Error(), " run on error event")
			t.taskErr = t.err(errOnExecute, t.onExecuteErr)
			if err := e.OnExecuteError(rctx, t.id, t.data, t.taskErr); err != nil {
				t.taskErr = t.err(errOnExecuteErr, err)
			}
		} else {
			t.log(t.id, "run on finished event")
			if err := e.OnFinish(rctx, t.id, t.data); err != nil {
				t.taskErr = t.err(errOnFinish, err)
			}
		}
		shouldCleanup = true
	case <-t.baseTask.signalPanic:
		t.taskErr = e.OnPanic(ctx, t.id, t.data)
		t.tb.collectPanic(true)
		shouldCleanup = true
	case <-t.baseTask.signalQuit:
		t.log(t.id, "signal terminated detected")
		t.baseTask.isQuit = true
	}
	if shouldCleanup {
		t.drain(rctx, false)
	}
	return
}

func (t *taskImpl) drain(ctx context.Context, quitting bool) error {
	if t.tb == nil {
		return errors.New("unable to drain task map already empty")
	}
	if quitting {
		t.signalQuit <- true
	}
	err := t.tb.removeTask(t.id)
	if err != nil {
		return err
	}
	t.log(t.id, fmt.Sprintf("draining task %s, current map length=%d", t.id, t.tb.getMapLength()))
	return nil
}

func (t *taskImpl) rescue(ctx context.Context) error {
	t.signalPanic <- true
	return nil
}

//##Region: Base Task implementation

func (b *baseTask) log(id string, args ...interface{}) {
	if b.verbose {
		id := fmt.Sprintf("[process_id:%s]", id)
		msg := fmt.Sprint(args...)
		log.Println(id, msg)
	}
}

func (b *baseTask) err(taskErr taskErrType, err error) error {
	return fmt.Errorf("%s: %s", taskErr.Error(), err.Error())
}

//##EndRegion: Base Task implementation
