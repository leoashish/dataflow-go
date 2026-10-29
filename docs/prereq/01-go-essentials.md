# Prerequisite 01 — Go essentials for data processing

*Read this first if Go is new or rusty. ~2 hours with the exercises. Everything here is used in the chapters.*

## 1. Modules, packages and the layout of this course
- A **module** is a tree of packages with a `go.mod` (`module dataflow`, `go 1.22`). Import paths start with the module name: `dataflow/internal/pipeline`.
- A **package** is one directory. Names starting with a capital letter are *exported*; everything else is private to the package.
- `internal/` is special: only code inside this module may import it.
- A directory beginning with `_` (like `_solutions/`) is **ignored by `go build ./...` and `go test ./...`** — that is why the reference solutions can sit in the repository without being compiled.
- Tests live next to the code in `*_test.go`. An **external test package** (`package pipeline_test`) can only use the exported API — good practice, because it tests what callers see.

## 2. Values, slices, maps — what is really inside
```go
s := []int{1, 2, 3}        // a slice is {pointer, length, capacity} pointing into an array
t := s[:2]                 // t shares s's array: t[0] = 9 changes s[0]
t = append(t, 7)           // fits in capacity -> overwrites s[2]!  (a classic surprise)
u := append([]int(nil), s...)   // a true copy
m := map[string]int{}      // unordered; iteration order is RANDOM on purpose
v, ok := m["x"]            // the comma-ok form: ok tells "absent" from "zero value"
```
Rules of thumb: pre-size with `make([]T, 0, n)` when you know `n`; never rely on map order (sort the keys); a nil map can be read but writing panics; copy slices you hand out if the caller might modify them (Chapter 22 tests this).

## 3. Errors are values
```go
var ErrNotFound = errors.New("not found")
return fmt.Errorf("load %q: %w", name, ErrNotFound)   // %w WRAPS: the cause stays inspectable
if errors.Is(err, ErrNotFound) { ... }                 // walks the chain
var pe *fs.PathError; if errors.As(err, &pe) { ... }   // find a typed error in the chain
errors.Join(e1, e2)                                    // several errors in one (Go 1.20+)
```
The chapters use **sentinel errors** (`ErrCorrupt`, `ErrClosed`) so tests can say *which kind* of failure happened with `errors.Is`. `todo.NotImplemented` is the error every unfinished stub returns, and `testutil.ExpectError` refuses it — so a stub can never "pass" an error test by accident.

## 4. `defer`, `panic`, `recover`
`defer` runs when the function returns — use it for cleanup (`defer f.Close()`, `defer mu.Unlock()`, `defer os.RemoveAll(dir)`). Deferred calls run in last-in-first-out order and *also* when the function panics. `recover()` (only inside a deferred function) stops a panic: Chapters 5 and 21 use it so one bad record or task cannot crash a whole pipeline.

## 5. Generics (Go 1.18+)
```go
func Map[T, R any](in []T, f func(T) R) []R { ... }
type Reservoir[T any] struct{ sample []T }
```
Used for pipeline stages (`pipeline.Map`), `KWayMerge[T]` and `Reservoir[T]`. Type parameters are inferred at the call site; constraints (`any`, `comparable`, `cmp.Ordered`) say what the type must support.

## 6. Methods, interfaces, embedding
Interfaces are satisfied implicitly: a type with the right methods *is* the interface. Keep them small (`io.Reader` has one method). Methods on a pointer receiver (`func (g *Group) Wait()`) can modify the value; **copying a struct that contains a `sync.Mutex` copies the lock** (`go vet` warns) — always pass such types by pointer.

## 7. The tools you will use every day
| Command | What it does |
|---|---|
| `go test ./internal/pipeline -run Ch01 -v` | run only chapter-1 tests, verbosely (`-run` takes a regular expression) |
| `go test ./... -race` | the **race detector**: finds unsynchronised memory access (Prerequisite 02). Use it always. |
| `go test -count=20 -run X` | repeat a test 20 times: the way to flush out flaky concurrency bugs |
| `go test -timeout 60s` | abort a hung run (this course also installs a 90-second watchdog) |
| `go vet ./...` | static checks (copied locks, wrong printf verbs, unreachable code) |
| `gofmt -l .` / `gofmt -w .` | list / fix badly formatted files |
| `go test -bench . -benchmem` | benchmarks with allocation counts |
| `go test -cpuprofile cpu.out` then `go tool pprof cpu.out` | where does the time go? |

## 8. How the tests in this course are written
- **Table-driven**: a slice of cases and a loop — add a case, not a function.
- **Helpers** call `t.Helper()` so failures point at *your* line, and `t.Fatalf` stops the test (use `t.Errorf` to continue and report several problems).
- **No sleeps for synchronisation.** Concurrency tests wait on channels or counters with a timeout, never `time.Sleep(100ms)` and hope.
- **Deterministic randomness**: `testutil.Rand(seed)` — a failing run can be reproduced.
- **A test must be able to fail.** The course's tests were checked by planting bugs on purpose (mutation testing) — adopt the habit for your own.

## Exercises
1. Write `func Dedup[T comparable](xs []T) []T` that keeps first occurrences in order. Add table tests including an empty and a nil slice.
2. Explain why `a := []int{1,2,3}; b := a[:1]; b = append(b, 99)` changes `a`, and how to avoid it.
3. Wrap an error three levels deep with `%w` and find the root with `errors.Is`.
4. Run `go test ./... -run Ch01 -v` now: every test should *fail cleanly* (no panic, no hang). That is the course's starting state.
