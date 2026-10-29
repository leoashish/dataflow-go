package dag_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dataflow/internal/dag"
	"dataflow/internal/testutil"
)

// Chapter 21 -- a DAG orchestrator.   Run:  go test ./internal/dag -run Ch21 -race -v

func ok(context.Context) error { return nil }

func task(name string, deps ...string) dag.Task { return dag.Task{Name: name, Deps: deps, Run: ok} }

// runDAG calls g.Run but refuses to wait forever: a scheduler that deadlocks (waiting for a result that never arrives) would
// otherwise freeze `go test` for its default ten minutes.
func runDAG(t *testing.T, g *dag.Graph, ctx context.Context, maxParallel int) (map[string]dag.Result, error) {
	t.Helper()
	type outcome struct {
		results map[string]dag.Result
		err     error
	}
	done := make(chan outcome, 1)
	go func() { r, err := g.Run(ctx, maxParallel); done <- outcome{r, err} }()
	select {
	case o := <-done:
		return o.results, o.err
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return within 10s: the scheduler is deadlocked (it is waiting for a task result that will never arrive, or for tasks it never started)")
		return nil, nil
	}
}

func mustNew(t *testing.T, tasks ...dag.Task) *dag.Graph {
	t.Helper()
	g, err := dag.New(tasks)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return g
}

// ---------------- validation ----------------

func TestCh21_New_RejectsInvalidGraphs(t *testing.T) {
	tests := []struct {
		name  string
		tasks []dag.Task
		want  error
		text  string // words the message must contain
	}{
		{"empty name", []dag.Task{{Name: "", Run: ok}}, dag.ErrInvalidTask, ""},
		{"nil run", []dag.Task{{Name: "a"}}, dag.ErrInvalidTask, "a"},
		{"duplicate", []dag.Task{task("a"), task("b"), task("a")}, dag.ErrDuplicateTask, "a"},
		{"unknown dependency", []dag.Task{task("a", "ghost")}, dag.ErrUnknownDependency, "ghost"},
		{"self dependency", []dag.Task{task("a", "a")}, dag.ErrCycle, "a -> a"},
		{"three-cycle", []dag.Task{task("a", "c"), task("b", "a"), task("c", "b"), task("ok")}, dag.ErrCycle, "a -> c -> b -> a"},
	}
	for _, c := range tests {
		g, err := dag.New(c.tasks)
		if err == nil || g != nil {
			t.Fatalf("%s: New = (%v, %v), want an error", c.name, g, err)
		}
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: error %v does not wrap %v", c.name, err, c.want)
		}
		if !strings.Contains(err.Error(), c.text) {
			t.Fatalf("%s: error %q should mention %q", c.name, err, c.text)
		}
	}
}

func TestCh21_New_ACycleBehindACleanPrefixIsStillFound(t *testing.T) {
	// start -> x <-> y : 'start' orders fine, x and y never become ready
	_, err := dag.New([]dag.Task{task("start"), task("x", "start", "y"), task("y", "x")})
	if !errors.Is(err, dag.ErrCycle) || !strings.Contains(err.Error(), "x") || !strings.Contains(err.Error(), "y") {
		t.Fatalf("error = %v, want ErrCycle naming x and y", err)
	}
}

func TestCh21_EmptyGraphIsValid(t *testing.T) {
	g := mustNew(t)
	if len(g.Order()) != 0 || len(g.Levels()) != 0 {
		t.Fatalf("Order=%v Levels=%v", g.Order(), g.Levels())
	}
	res, err := runDAG(t, g, context.Background(), 2)
	if err != nil || len(res) != 0 {
		t.Fatalf("Run = (%v, %v)", res, err)
	}
}

// ---------------- order and levels ----------------

func TestCh21_Order_IsTopological_AndBreaksTiesAlphabetically(t *testing.T) {
	g := mustNew(t, task("d", "b", "c"), task("c", "a"), task("b", "a"), task("a"), task("e"))
	if got := g.Order(); !slices.Equal(got, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("Order = %v, want [a b c d e]", got)
	}
	// the alphabetical rule applies among READY tasks only: 'a' must wait for 'z'
	g2 := mustNew(t, task("z"), task("a", "z"), task("m"))
	if got := g2.Order(); !slices.Equal(got, []string{"m", "z", "a"}) {
		t.Fatalf("Order = %v, want [m z a]", got)
	}
}

func randomDAG(t *testing.T, n int, seed int64) []dag.Task {
	t.Helper()
	rnd := testutil.Rand(seed)
	tasks := make([]dag.Task, n)
	for i := range tasks {
		var deps []string
		for j := 0; j < i; j++ {
			if rnd.Intn(8) == 0 { // edges only point to LOWER indexes: acyclic by construction
				deps = append(deps, fmt.Sprintf("t%03d", j))
			}
		}
		tasks[i] = task(fmt.Sprintf("t%03d", i), deps...)
	}
	rnd.Shuffle(len(tasks), func(i, j int) { tasks[i], tasks[j] = tasks[j], tasks[i] }) // the input order must not matter
	return tasks
}

func TestCh21_Order_PutsEveryTaskAfterItsDependencies(t *testing.T) {
	tasks := randomDAG(t, 150, 1)
	g := mustNew(t, tasks...)
	order := g.Order()
	if len(order) != len(tasks) {
		t.Fatalf("Order has %d tasks, want %d", len(order), len(tasks))
	}
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	for _, task := range tasks {
		for _, d := range task.Deps {
			if pos[d] >= pos[task.Name] {
				t.Fatalf("%s is ordered before its dependency %s", task.Name, d)
			}
		}
	}
	if !slices.Equal(order, mustNew(t, randomDAG(t, 150, 1)...).Order()) {
		t.Fatal("Order is not deterministic for the same graph")
	}
}

func TestCh21_Levels_AreLongestChainsFromTheRoots(t *testing.T) {
	g := mustNew(t, task("a"), task("b", "a"), task("c", "a"), task("e", "a"), task("d", "b", "c"), task("f", "d", "a"))
	want := [][]string{{"a"}, {"b", "c", "e"}, {"d"}, {"f"}}
	got := g.Levels()
	if len(got) != len(want) {
		t.Fatalf("Levels = %v, want %v", got, want)
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Fatalf("Levels = %v, want %v ('f' depends on 'a' directly but its LONGEST chain is a-b-d-f, so level 3)", got, want)
		}
	}
}

// ---------------- running ----------------

// tracker records WHEN each task started and finished using one monotonic counter, so the tests can check ordering
// invariants without depending on goroutine scheduling.
type tracker struct {
	mu       sync.Mutex
	seq      int64
	start    map[string]int64
	end      map[string]int64
	cur, max int
}

func newTracker() *tracker { return &tracker{start: map[string]int64{}, end: map[string]int64{}} }

func (tr *tracker) begin(name string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.seq++
	tr.start[name] = tr.seq
	tr.cur++
	tr.max = max(tr.max, tr.cur)
}

func (tr *tracker) finish(name string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.seq++
	tr.end[name] = tr.seq
	tr.cur--
}

func (tr *tracker) peak() int { tr.mu.Lock(); defer tr.mu.Unlock(); return tr.max }

func tracked(tr *tracker, name string, work time.Duration, deps ...string) dag.Task {
	return dag.Task{Name: name, Deps: deps, Run: func(context.Context) error {
		tr.begin(name)
		defer tr.finish(name)
		time.Sleep(work)
		return nil
	}}
}

func TestCh21_Run_NeverStartsATaskBeforeItsDependenciesHaveFinished(t *testing.T) {
	testutil.NoLeaks(t)
	tr := newTracker()
	shape := randomDAG(t, 120, 2)
	tasks := make([]dag.Task, len(shape))
	for i, s := range shape {
		tasks[i] = tracked(tr, s.Name, 200*time.Microsecond, s.Deps...)
	}
	g := mustNew(t, tasks...)
	results, err := runDAG(t, g, context.Background(), 8)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, task := range tasks {
		if r := results[task.Name]; r.Status != dag.Succeeded || r.Attempts != 1 || r.Err != nil {
			t.Fatalf("%s: %+v", task.Name, r)
		}
		for _, d := range task.Deps {
			if tr.end[d] >= tr.start[task.Name] {
				t.Fatalf("%s started (event %d) before its dependency %s finished (event %d)", task.Name, tr.start[task.Name], d, tr.end[d])
			}
		}
	}
	if len(results) != 120 {
		t.Fatalf("%d results, want 120", len(results))
	}
}

func TestCh21_Run_IndependentTasksReallyRunInParallel(t *testing.T) {
	testutil.NoLeaks(t)
	var started sync.WaitGroup
	started.Add(3)
	allStarted := make(chan struct{})
	go func() { started.Wait(); close(allStarted) }()
	rendezvous := func(ctx context.Context) error {
		started.Done()
		select { // each task waits until ALL THREE are running at the same moment
		case <-allStarted:
			return nil
		case <-time.After(3 * time.Second):
			return errors.New("the three tasks never ran at the same time")
		}
	}
	g := mustNew(t, dag.Task{Name: "a", Run: rendezvous}, dag.Task{Name: "b", Run: rendezvous}, dag.Task{Name: "c", Run: rendezvous})
	if _, err := runDAG(t, g, context.Background(), 3); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestCh21_Run_NeverExceedsMaxParallel(t *testing.T) {
	testutil.NoLeaks(t)
	for _, limit := range []int{1, 2, 5} {
		tr := newTracker()
		var tasks []dag.Task
		for i := 0; i < 20; i++ {
			tasks = append(tasks, tracked(tr, fmt.Sprintf("t%02d", i), 2*time.Millisecond))
		}
		results, err := runDAG(t, mustNew(t, tasks...), context.Background(), limit)
		if err != nil || len(results) != 20 {
			t.Fatalf("limit %d: Run = (%d results, %v)", limit, len(results), err)
		}
		if tr.peak() > limit || tr.peak() < 1 {
			t.Fatalf("limit %d: %d tasks ran at once", limit, tr.peak())
		}
	}
}

func TestCh21_Run_RejectsMaxParallelBelowOne(t *testing.T) {
	g := mustNew(t, task("a"))
	for _, n := range []int{0, -1} {
		if _, err := runDAG(t, g, context.Background(), n); !errors.Is(err, dag.ErrBadParallelism) {
			t.Fatalf("maxParallel %d: error = %v, want ErrBadParallelism", n, err)
		}
	}
}

func TestCh21_Run_AFailureSkipsEverythingDownstream_ButNotTheOtherBranches(t *testing.T) {
	testutil.NoLeaks(t)
	boom := errors.New("boom")
	var ran sync.Map
	mark := func(name string, err error) func(context.Context) error {
		return func(context.Context) error { ran.Store(name, true); return err }
	}
	g := mustNew(t,
		dag.Task{Name: "a", Run: mark("a", nil)},
		dag.Task{Name: "b", Deps: []string{"a"}, Run: mark("b", boom)},           // fails
		dag.Task{Name: "c", Deps: []string{"a"}, Run: mark("c", nil)},            // sibling branch: must still run
		dag.Task{Name: "d", Deps: []string{"b"}, Run: mark("d", nil)},            // downstream of the failure
		dag.Task{Name: "d2", Deps: []string{"d"}, Run: mark("d2", nil)},          // transitively downstream
		dag.Task{Name: "join", Deps: []string{"b", "c"}, Run: mark("join", nil)}, // one parent failed, one succeeded
		dag.Task{Name: "e", Run: mark("e", nil)},                                 // independent
	)
	results, err := runDAG(t, g, context.Background(), 4)

	want := map[string]dag.Status{"a": dag.Succeeded, "b": dag.Failed, "c": dag.Succeeded, "d": dag.Skipped, "d2": dag.Skipped, "join": dag.Skipped, "e": dag.Succeeded}
	for name, status := range want {
		if got := results[name].Status; got != status {
			t.Errorf("%s: %v, want %v", name, got, status)
		}
	}
	for _, name := range []string{"d", "d2", "join"} {
		if _, wasRun := ran.Load(name); wasRun || results[name].Attempts != 0 {
			t.Errorf("%s was skipped, so its Run must never be called (attempts %d)", name, results[name].Attempts)
		}
	}
	if !errors.Is(results["b"].Err, boom) {
		t.Errorf("b's error = %v, want boom", results["b"].Err)
	}
	if !errors.Is(err, dag.ErrRunFailed) || !errors.Is(err, boom) || !strings.Contains(err.Error(), `"b"`) {
		t.Fatalf("Run error = %v, want one wrapping ErrRunFailed and boom and naming task b", err)
	}
}

func TestCh21_Run_RetriesUseUpTheBudgetAndStopAtTheFirstSuccess(t *testing.T) {
	testutil.NoLeaks(t)
	flaky := func(failures int) (func(context.Context) error, *atomic.Int64) {
		var calls atomic.Int64
		return func(context.Context) error {
			if int(calls.Add(1)) <= failures {
				return fmt.Errorf("transient failure %d", calls.Load())
			}
			return nil
		}, &calls
	}
	twice, _ := flaky(2)
	always, alwaysCalls := flaky(1000)
	once, _ := flaky(0)
	g := mustNew(t,
		dag.Task{Name: "recovers", Retries: 2, Run: twice}, // fails twice, third attempt works
		dag.Task{Name: "exhausts", Retries: 1, Run: always},
		dag.Task{Name: "noretry", Retries: 0, Run: always},
		dag.Task{Name: "fine", Retries: 5, Run: once},
	)
	results, err := runDAG(t, g, context.Background(), 2)
	if !errors.Is(err, dag.ErrRunFailed) {
		t.Fatalf("Run error = %v", err)
	}
	if r := results["recovers"]; r.Status != dag.Succeeded || r.Attempts != 3 {
		t.Errorf("recovers: %+v, want Succeeded after 3 attempts", r)
	}
	if r := results["exhausts"]; r.Status != dag.Failed || r.Attempts != 2 || r.Err == nil {
		t.Errorf("exhausts: %+v, want Failed after 1 + 1 retry = 2 attempts", r)
	}
	if r := results["noretry"]; r.Status != dag.Failed || r.Attempts != 1 {
		t.Errorf("noretry: %+v, want Failed after exactly 1 attempt", r)
	}
	if r := results["fine"]; r.Status != dag.Succeeded || r.Attempts != 1 {
		t.Errorf("fine: %+v, want Succeeded on the first attempt", r)
	}
	if alwaysCalls.Load() != 3 { // shared by "exhausts" (2 attempts) and "noretry" (1)
		t.Errorf("the failing function ran %d times in total, want 3", alwaysCalls.Load())
	}
}

func TestCh21_Run_APanicIsAFailedAttempt_NotACrash(t *testing.T) {
	testutil.NoLeaks(t)
	g := mustNew(t,
		dag.Task{Name: "bomb", Run: func(context.Context) error { panic("kaboom") }},
		task("after", "bomb"),
		task("other"),
	)
	results, err := runDAG(t, g, context.Background(), 2)
	if !errors.Is(err, dag.ErrRunFailed) {
		t.Fatalf("Run error = %v", err)
	}
	if r := results["bomb"]; r.Status != dag.Failed || r.Err == nil || !strings.Contains(r.Err.Error(), "panic") {
		t.Fatalf("bomb: %+v, want Failed with an error mentioning the panic", r)
	}
	if results["after"].Status != dag.Skipped || results["other"].Status != dag.Succeeded {
		t.Fatalf("after=%v other=%v", results["after"].Status, results["other"].Status)
	}
}

func TestCh21_Run_CancellationStopsNewWork_AndReportsWhatHappened(t *testing.T) {
	testutil.NoLeaks(t)
	var afterRan atomic.Bool
	started := make(chan struct{})
	g := mustNew(t,
		dag.Task{Name: "block", Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done() // a well-behaved task watches its context
			return ctx.Err()
		}},
		dag.Task{Name: "child", Deps: []string{"block"}, Run: ok},
		dag.Task{Name: "later", Run: func(context.Context) error { afterRan.Store(true); return nil }}, // queued behind 'block' (maxParallel = 1)
	)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-started; cancel() }()

	type outcome struct {
		results map[string]dag.Result
		err     error
	}
	done := make(chan outcome, 1)
	go func() { r, err := g.Run(ctx, 1); done <- outcome{r, err} }()

	var out outcome
	select {
	case out = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the context was canceled")
	}
	if afterRan.Load() {
		t.Fatal("'later' ran although the context had been canceled before it could start")
	}
	if r := out.results["block"]; r.Status != dag.Failed || !errors.Is(r.Err, context.Canceled) {
		t.Errorf("block: %+v, want Failed with context.Canceled", r)
	}
	if got := out.results["child"].Status; got != dag.Skipped {
		t.Errorf("child: %v, want Skipped (its dependency failed)", got)
	}
	if r := out.results["later"]; r.Status != dag.Canceled || !errors.Is(r.Err, context.Canceled) || r.Attempts != 0 {
		t.Errorf("later: %+v, want Canceled with ctx.Err() and 0 attempts", r)
	}
	if !errors.Is(out.err, dag.ErrRunFailed) {
		t.Errorf("Run error = %v", out.err)
	}
}

func TestCh21_Run_AlreadyCanceledContextRunsNothing(t *testing.T) {
	testutil.NoLeaks(t)
	var calls atomic.Int64
	count := func(context.Context) error { calls.Add(1); return nil }
	g := mustNew(t, dag.Task{Name: "a", Run: count}, dag.Task{Name: "b", Deps: []string{"a"}, Run: count})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results, err := runDAG(t, g, ctx, 2)
	if !errors.Is(err, dag.ErrRunFailed) || calls.Load() != 0 {
		t.Fatalf("err=%v calls=%d, want ErrRunFailed and no task started", err, calls.Load())
	}
	for _, name := range []string{"a", "b"} {
		if results[name].Status != dag.Canceled {
			t.Errorf("%s: %v, want Canceled", name, results[name].Status)
		}
	}
}

func TestCh21_Run_AGraphCanBeRunTwice(t *testing.T) {
	var calls atomic.Int64
	g := mustNew(t, dag.Task{Name: "a", Run: func(context.Context) error { calls.Add(1); return nil }}, task("b", "a"))
	for i := 0; i < 2; i++ {
		if _, err := runDAG(t, g, context.Background(), 2); err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("a ran %d times over two runs, want 2", calls.Load())
	}
}
