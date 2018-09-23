package gobucket

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

//TaskBucket works as a bucket implementation for tasks pool
type TaskBucket interface {
	Fill(ctx context.Context, taskType TaskType, id string, data interface{}, e Executor) error
	Drain(ctx context.Context, id string) error
	removeTask(id string) error
	getMapLength() int
}

//Executor defines a client task definition
type Executor interface {
	OnExecute(ctx context.Context, id string, data interface{}) error                          //perform something
	OnFinish(ctx context.Context, id string, data interface{}) error                           //clean up
	OnTaskExhausted(ctx context.Context, id string, data interface{}) error                    //context deadline happens
	OnExecuteError(ctx context.Context, id string, data interface{}, onExecuteErr error) error //error while executing
}

//taskBucketImpl task bucket object holder and methods
type taskBucketImpl struct {
	mux    sync.Mutex
	tasks  map[string]task
	config *BucketConfig
}

//NewTaskBucket creates new task bucket
//args:
//	cfg: configuration of task bucket
//returns:
//	task bucket
func NewTaskBucket(cfg *BucketConfig) TaskBucket {
	return &taskBucketImpl{
		tasks:  make(map[string]task, cfg.MaxBucket),
		config: cfg,
	}
}

//Fill puts the task to task buffer, run the job right away
//args:
//	ctx: context passed
//	tt: task type
//	id: identity of the task
//	e: the task executor job
//returns:
//	fill operation error
func (tb *taskBucketImpl) Fill(ctx context.Context, tt TaskType, id string, data interface{}, e Executor) error {
	var isFull bool
	tb.mux.Lock()
	isFull = len(tb.tasks) > tb.config.MaxBucket-1
	tb.mux.Unlock()
	if isFull {
		return fmt.Errorf("unable to fill bucket for task=%s, max=%d", id, tb.config.MaxBucket)
	}
	//prepare the tax
	task := newTask(tt, id, tb.config, e, data, tb)
	tb.mux.Lock()
	tb.tasks[id] = task
	tb.mux.Unlock()
	//run the task: go routine
	go task.run(ctx)
	return nil
}

//Drain removes the task from the task bucket
//args:
//	ctx: passed ctx
//	id: task identity
//return:
//	error status
func (tb *taskBucketImpl) Drain(ctx context.Context, id string) error {
	tb.mux.Lock()
	task, ok := tb.tasks[id]
	tb.mux.Unlock()
	if ok {
		return task.drain(ctx, true)
	}
	return fmt.Errorf("task with id %s is not found", id)
}

//removeTask remove the task from internal task bucket
func (tb *taskBucketImpl) removeTask(id string) error {
	tb.mux.Lock()
	isNill := (tb.tasks == nil)
	tb.mux.Unlock()
	if isNill {
		return errors.New("task already nil")
	}
	tb.mux.Lock()
	_, ok := tb.tasks[id]
	tb.mux.Unlock()

	if ok {
		tb.mux.Lock()
		delete(tb.tasks, id)
		tb.mux.Unlock()
		return nil
	}
	return fmt.Errorf("task with id %s is not exists, unable to remove", id)
}

//getMapLength gets the actual length of the map
func (tb *taskBucketImpl) getMapLength() int {
	tb.mux.Lock()
	ln := len(tb.tasks)
	tb.mux.Unlock()
	return ln
}
