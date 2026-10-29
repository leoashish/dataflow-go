package reliable

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

func Run(ctx context.Context, in <-chan Message, h Handler, cfg Config) (Result, error) {
	workers := max(cfg.Workers, 1)
	maxAttempts := max(cfg.MaxAttempts, 1)
	sleeper := cfg.Sleeper
	if sleeper == nil {
		sleeper = SystemSleeper{}
	}

	var (
		mu  sync.Mutex
		res Result
		wg  sync.WaitGroup
	)
	record := func(update func(*Result)) {
		mu.Lock()
		defer mu.Unlock()
		update(&res)
	}

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case m, ok := <-in:
					if !ok {
						return
					}
					processOne(ctx, m, h, maxAttempts, cfg.Backoff, sleeper, record)
				}
			}
		}()
	}
	wg.Wait()

	sort.Slice(res.DeadLetters, func(i, j int) bool { return res.DeadLetters[i].Message.ID < res.DeadLetters[j].Message.ID })
	return res, ctx.Err()
}

func processOne(ctx context.Context, m Message, h Handler, maxAttempts int, backoff func(int) time.Duration, sleeper Sleeper, record func(func(*Result))) {
	for attempt := 1; ; attempt++ {
		err, panicked := call(ctx, h, m)
		if ctx.Err() != nil && err != nil {
			record(func(r *Result) { r.TotalAttempts++ })
			return // shutting down: the message is unfinished, not failed
		}
		record(func(r *Result) { r.TotalAttempts++ })

		switch {
		case err == nil:
			record(func(r *Result) { r.Succeeded++ })
			return
		case panicked || errors.Is(err, ErrPermanent) || attempt >= maxAttempts:
			record(func(r *Result) {
				r.DeadLetters = append(r.DeadLetters, DeadLetter{Message: m, Err: err, Attempts: attempt})
			})
			return
		}
		if backoff != nil {
			if sleeper.Sleep(ctx, backoff(attempt)) != nil {
				return // cancelled while waiting
			}
		}
	}
}

// call runs the handler, converting a panic into an error.
func call(ctx context.Context, h Handler, m Message) (err error, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			err, panicked = fmt.Errorf("handler panicked: %v", r), true
		}
	}()
	return h(ctx, m), false
}
