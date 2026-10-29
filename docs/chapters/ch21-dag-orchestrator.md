# Chapter 21 — A DAG orchestrator: dependencies, parallelism, retries and failure

| | |
|---|---|
| **Build** | `New`, `Order`, `Levels` (`graph.go`); `Run` (`run.go`) — `internal/dag/` |
| **Given** | `Task`, `Status`, `Result`, the error sentinels (`dag.go`) |
| **Tests** | `internal/dag/dag_test.go` — 16 tests |
| **Run** | `go test ./internal/dag -run Ch21 -race -v` |
| **Prerequisites** | Chapters 01–02 and 05; prereq 02 (concurrency), prereq 04 (graphs) |
| **Time** | ~4 h |

## 1. Why this matters

A data pipeline is never one job: *extract orders, extract customers → join → aggregate → publish → notify*. Something must decide **what runs when**, run independent steps in parallel, stop downstream steps when an upstream one fails, retry flaky ones, and give you a report. That something is an **orchestrator** — Airflow, Dagster, Prefect, Argo, Luigi, dbt's runner, Make. At its heart it is a **topological sort** plus a **bounded-concurrency scheduler**, and the interesting part is getting the *failure semantics* right.

## 2. The lesson, step by step

### Step 1 — dependencies form a graph; it must have no cycles
Tasks are nodes; "B needs A" is an edge. A valid schedule exists iff the graph is a **DAG** (directed acyclic graph). A cycle (`a → b → c → a`) has no valid order: nothing in it can ever start. Catch it at *construction time* with a message that **shows the loop** — far more useful than "cycle detected".

### Step 2 — Kahn's algorithm: ordering and cycle detection in one
```
count each task's unfinished dependencies
ready = tasks with 0
while ready is not empty: take one, output it, and decrement the count of everything that depends on it;
                          anything that reaches 0 becomes ready
if some tasks were never output ⇒ they are on, or behind, a cycle
```
O(V + E). To report the cycle: every leftover task still has a leftover dependency, so start anywhere and keep stepping to a leftover dependency — you must revisit a task, and that path is a cycle.

### Step 3 — determinism: a tie-break
Several tasks are ready at once; which goes first? If you leave it to map iteration order (random in Go!) your `Order()` changes run to run and tests flake. Rule: **take the alphabetically smallest ready task**. Note the rule only applies among *ready* tasks: `z; a→z; m` orders as `m z a`, not `a m z`.

### Step 4 — levels and the critical path
`Levels()` groups tasks by **longest dependency chain**: level 0 = no dependencies; otherwise 1 + max(level of dependencies). Everything in one level could run in parallel. The number of levels is the **critical path** — *the fastest the pipeline can possibly finish, however many workers you have*. A task depending on a level-0 task *and* a level-2 task is at level 3, not 1 (the test has exactly this trap).

### Step 5 — the scheduler: one loop owns all the state
```
loop until every task has a result:
    start ready tasks while  running < maxParallel   (never more!)
    wait for ONE task to finish
    success → decrement its dependents; newly ready ones join the queue
    failure → mark every TRANSITIVE dependent Skipped
```
The trick that removes all locking: **only this loop touches the bookkeeping**; worker goroutines do nothing but run the task and send the result on a channel. No mutex, no data race, easy to reason about. (Compare Chapter 1's "share memory by communicating".)

### Step 6 — failure semantics (the heart of an orchestrator)
- A task that fails **after its retries** makes all its *transitive* dependents **Skipped** — never started, `Attempts == 0`. Running them on missing inputs would produce wrong data.
- **Independent branches continue.** One broken report must not stop the other forty.
- A task with two parents, one failed and one still running: skipped *now*; when the other parent later succeeds, it must **not** be started (the code must ignore results for already-finished tasks).
- **Retries**: `1 + Retries` attempts, stop at the first success, no more retries once the context is done.
- **Panics** are failures, not process crashes: recover per attempt and keep the error.

### Step 7 — cancellation
`ctx` canceled ⇒ **tasks that have not started become `Canceled`** (never run), tasks that are running are **waited for** (they should watch `ctx` and return), and `Run` returns **only when every goroutine has finished** — no leaks (the tests check). A task that failed *because* it was canceled is still `Failed` and still skips its dependents; tasks queued behind it are `Canceled`. Distinguishing *Failed / Skipped / Canceled* gives an operator the right next step: fix it / fix the upstream / just re-run.

### Step 8 — testing a scheduler without flaky tests
You can't assert "task X started at time T". Assert **invariants** instead: (1) a **monotonic event counter** records start/end of every task, and *no task starts before all its dependencies' end events*; (2) a counter of "currently running" never exceeds `maxParallel`; (3) a **rendezvous** (all N tasks wait until all N are running) *proves* parallelism is real. These hold for every legal interleaving, so they never flake. Also note `runDAG` in the tests: it runs `Run` under a 10-second deadline so a deadlock in *your* scheduler is a clear failure, not a frozen terminal.

## 3. Your challenge
`New` (validation + cycle detection), `Order`, `Levels`, then `Run`.

## 4. Hints
<details><summary>Hint 1 — validation order</summary>

Per task: empty name / nil `Run` → `ErrInvalidTask`; duplicate → `ErrDuplicateTask`. Only after *all* names are known check dependencies (`ErrUnknownDependency`, naming both tasks), then run Kahn for cycles. Wrap with `fmt.Errorf("%w: ...", Sentinel)`.
</details>
<details><summary>Hint 2 — sorted names and dependents</summary>

Keep `names` sorted and a `dependents map[string][]string` (sorted). Everything deterministic follows from iterating those instead of the raw map.
</details>
<details><summary>Hint 3 — the Run loop</summary>

State: `remaining map[string]int`, `ready []string`, `results map`, `running int`, `done chan finished`. Launch while `running < maxParallel && len(ready) > 0 && ctx.Err() == nil`. If `running == 0` after that, nothing can progress: mark every task without a result `Canceled`. Otherwise receive one result.
</details>
<details><summary>Hint 4 — the final error</summary>

`fmt.Errorf("%w: %d failed, %d skipped, %d canceled: %w", ErrRunFailed, f, s, c, errors.Join(taskErrs...))` — Go 1.20+ allows two `%w`, so `errors.Is` finds both `ErrRunFailed` and each task's own error.
</details>

## 5. Stretch challenges
1. **Backoff between retries** (reuse Chapter 5's `Sleeper` so tests stay instant).
2. **Per-task timeouts** via `context.WithTimeout`; a timed-out task is `Failed` with `context.DeadlineExceeded`.
3. **Resume / idempotent re-run:** `Run` takes the previous `results` and skips tasks already `Succeeded` — the heart of Airflow's "clear failed tasks and re-run".
4. **Dynamic fan-out:** a task that returns more tasks to schedule (like Airflow's dynamic task mapping).
5. **Critical-path scheduling:** when more tasks are ready than slots, start the ones on the longest remaining chain first. Measure the makespan against alphabetical order on a random DAG.
6. **Resource classes:** at most 2 "heavy" tasks at once, regardless of `maxParallel`.
7. **Render the graph** as Mermaid/Graphviz text with statuses coloured.

## 6. Reflect
- Why does the scheduler loop keep all state itself instead of sharing a map behind a mutex?
- A task has two parents; one fails, one succeeds a second later. Walk through exactly what must (not) happen.
- What is the critical path, and why does adding workers not make a pipeline faster than it?
- Why distinguish *Skipped* from *Canceled* from *Failed*?
- Your nightly DAG has 400 tasks and takes 6 hours. How would you find out whether more parallelism would help?
