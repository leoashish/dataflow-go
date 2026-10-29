package dag

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

type finished struct {
	name   string
	result Result
}

func (g *Graph) Run(ctx context.Context, maxParallel int) (map[string]Result, error) {
	if maxParallel < 1 {
		return nil, ErrBadParallelism
	}
	results := make(map[string]Result, len(g.tasks))
	remaining := map[string]int{}
	var ready []string
	for _, name := range g.names {
		unique := map[string]bool{}
		for _, d := range g.tasks[name].Deps {
			unique[d] = true
		}
		remaining[name] = len(unique)
		if len(unique) == 0 {
			ready = append(ready, name)
		}
	}

	done := make(chan finished)
	running := 0
	for len(results) < len(g.tasks) {
		for running < maxParallel && len(ready) > 0 && ctx.Err() == nil {
			sort.Strings(ready)
			name := ready[0]
			ready = ready[1:]
			running++
			go func() { done <- finished{name, g.execute(ctx, g.tasks[name])} }()
		}
		if running == 0 {
			// nothing is running and nothing may be launched: only a canceled context can leave tasks unfinished
			for _, name := range g.names {
				if _, ok := results[name]; !ok {
					results[name] = Result{Status: Canceled, Err: ctx.Err()}
				}
			}
			break
		}
		f := <-done
		running--
		results[f.name] = f.result
		if f.result.Status == Succeeded {
			seen := map[string]bool{}
			for _, dep := range g.dependents[f.name] {
				if seen[dep] {
					continue
				}
				seen[dep] = true
				if _, finished := results[dep]; finished {
					continue // already skipped because another dependency failed
				}
				if remaining[dep]--; remaining[dep] == 0 {
					ready = append(ready, dep)
				}
			}
			continue
		}
		g.skipDependents(f.name, results) // a failure poisons everything downstream
	}

	var failures []error
	var failed, skipped, canceled int
	for _, name := range g.names {
		r := results[name]
		switch r.Status {
		case Failed:
			failed++
			failures = append(failures, fmt.Errorf("task %q: %w", name, r.Err))
		case Skipped:
			skipped++
		case Canceled:
			canceled++
		}
	}
	if failed+skipped+canceled == 0 {
		return results, nil
	}
	return results, fmt.Errorf("%w: %d failed, %d skipped, %d canceled: %w", ErrRunFailed, failed, skipped, canceled, errors.Join(failures...))
}

func (g *Graph) skipDependents(name string, results map[string]Result) {
	queue := []string{name}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, dep := range g.dependents[cur] {
			if _, ok := results[dep]; !ok {
				results[dep] = Result{Status: Skipped}
				queue = append(queue, dep)
			}
		}
	}
}

func (g *Graph) execute(ctx context.Context, t Task) Result {
	var err error
	attempts := 0
	for attempts <= t.Retries {
		attempts++
		if err = safeRun(ctx, t); err == nil {
			return Result{Status: Succeeded, Attempts: attempts}
		}
		if ctx.Err() != nil {
			break // no more retries once the context is done
		}
	}
	return Result{Status: Failed, Err: err, Attempts: attempts}
}

func safeRun(ctx context.Context, t Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in task %q: %v", t.Name, r)
		}
	}()
	return t.Run(ctx)
}
