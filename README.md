## GoBucket

Gobucket is an easy process background job for Go project. Gobucket keeps the job process in memory, and run it right away, or wait until a certain time has been met. 

### Usage:

```
taskBucket := gobucket.NewTaskBucket(&gobucket.BucketConfig{
		LifeSpan:  time.Second * 5,
		MaxBucket: 1024,
		Verbose:   true,
        RunAfter:  time.Second
})
```

This is the simple implementation for creating a `taskBucket` the task bucket will holds the job inside the memory as a map of task with a id (string) as an identifier. 

The task inside the bucket will be executed right away, so it is expected to not depend for each other. Each task will have its own life-span which will be removed by itself when it happens. This is the simple way to add the task to the bucket:

```
data := struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}{
    ID:   1,
	Name: "Johnson",
}
taskBucket.Fill(context.Background(), gobucket.ImmidiateTask, fmt.Sprintf("process::%d", proc), data, new(sampleExecutor))
```

At the moment, there is 2 type of task type:
1. Immidiate task: This is represented by `gobucket.ImmidiateTask`. This task will be executed right away, after being scheduled.
2. Time bomb task: This is represented by `gobucket.TimeBombTask`. This task will wait until the expected time before being executed using config `RunAfter`. Please be notified that the `LifeSpan` should be `>` than `RunAfter` so it can work without any problem.


`sampleExecutor` represent an executor, a core of single task job:

```

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
```

To remove the task from the bucket, it can use
```
taskBucket.Drain(context.Background(), id)
```
Drain should be call when the task is not yet exists. It will trigger `signal quit` and remove the task.

### Development:

Gobucket is expected to be a lightweight library for its implementation. However, this library is under development and require test to be used in production. If you are interested in more mature library which store the job in db such as redis, you can find alot of background process job support go-library in github.