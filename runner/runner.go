package runner

import (
	"context"
	"encoding/json"
	"log"
	"sync"
)

type (
	Runner struct {
		tasks map[string]*Task
		wg    *sync.WaitGroup
	}

	Task struct {
		Fn, Shutdown func(context.Context) error
		cancel       context.CancelFunc
		name         string
	}

	Interface interface {
		Name() string
		Task() *Task
	}

	Errors map[string]error
)

func New() Runner {
	return Runner{
		tasks: make(map[string]*Task),
		wg:    new(sync.WaitGroup),
	}
}

func (e Errors) Error() string {
	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(data)
}

func (r Runner) Register(v Interface) {
	r.AddTask(v.Name(), v.Task())
}

func (r Runner) AddTask(name string, task *Task) {
	r.addTask(name, task)
}

func (r Runner) addTask(name string, task *Task) {
	task.name = name
	r.tasks[name] = task
}

func (r Runner) Run(ctx context.Context) {
	r.run(ctx)
}

func (r Runner) run(ctx context.Context) {
	for name, task := range r.tasks {
		ctx, cancel := context.WithCancel(ctx)
		task.setCancel(cancel)
		log.Printf("running task %q", name)
		r.wg.Add(1)
		go task.run(ctx, r.wg)
	}
}

func (r Runner) Stop(ctx context.Context) error {
	return r.stopWithErrors(ctx)
}

func (r Runner) stopWithErrors(ctx context.Context) error {
	var errs Errors

	for _, task := range r.tasks {
		if err := task.stop(ctx); err != nil {
			if errs == nil {
				errs = make(Errors)
			}
			errs[task.name] = err
		}
	}

	return errs
}

func (r Runner) Wait() {
	r.wg.Wait()
}

func (t *Task) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := t.Fn(ctx); err != nil {
		log.Printf("task %q error: %s", t.name, err)
	}
}

func (t *Task) stop(ctx context.Context) error {
	if t.Shutdown != nil {
		return t.Shutdown(ctx)
	}
	t.cancel()
	return ctx.Err()
}

func (t *Task) setCancel(cancel context.CancelFunc) {
	t.cancel = cancel
}
