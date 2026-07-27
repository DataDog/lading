# Shutdown-safety scenario

This scenario tests that lading performs a clean, prompt graceful shutdown when
its own experiment timer elapses, even while a generator is stuck unable to reach
its destination. Shutdown correctness matters because lading owns the clock in
real deployments and must drain and exit on schedule.

## How it works

This scenario comprises the following components:

* lading, instrumented (system under test)
* workload (driver)

There is deliberately no sink. lading drives itself, so the workload only emits
`setup_complete`, bakes the checker, and idles.

lading owns the clock. The experiment timer fires at 15 seconds and signals
graceful shutdown, after which lading must drain its subsystems and exit cleanly
with `rc == 0`. To stress that drain, the tcp generator is aimed at
`127.0.0.1:1`, where nothing listens, so its worker sits in a busy reconnect loop
and never makes forward progress. This is the adversarial condition: a generator
that will not make progress on its own and could stall an unsafe drain.

`lading-entrypoint.sh` runs lading under a wall-clock `timeout` watchdog so a
prompt drain is distinguishable from a hang. The timer fires at 15 seconds,
`--max-shutdown-delay` is set to 60 seconds, and `timeout` kills lading at 35
seconds. A shutdown-responsive lading exits at ~15 seconds with `rc == 0`. A
lading that hangs the drain can only be released by its 60-second backstop, which
is longer than the 35-second watchdog, so the watchdog kills it first and yields
`rc == 124`. lading records its exit code to the shared volume, and the
workload's `anytime_lading_drained_bounded` checker reads it and asserts
`always(rc == 0)`. Any other code indicates a crash.

lading is also instrumented with SUT-side breadcrumbs along the graceful path
(began shutdown on timer, drain barrier returned, capture flushed, clean exit).
These trace how far shutdown progressed, so a regression that stalls the drain is
localized to the point where the breadcrumbs stop being reached.

# Caveats

1. `rc == 0` today confirms the outcome (prompt clean exit), not that every
   worker was individually drained. The generator's shutdown watcher is not a
   registered peer, so the drain barrier does not wait on it and the stuck worker
   is abandoned at runtime teardown. If the load-bearing watchers are ever made
   registered peers, a stuck generator would hang the drain and this scenario
   would catch it as `rc == 124`.
2. There are no node faults here. A node kill would preempt the graceful timer
   and hide the drain behavior. Global cpu and clock faults still apply.
3. The config is fixed and baked in. This scenario does not sample per-timeline
   configs.

# Assumptions

1. lading owns the clock and self-terminates on its experiment timer.
2. `--max-shutdown-delay` exceeds the watchdog margin, so reliance on the
   force-drop backstop surfaces as a watchdog kill rather than a clean exit.
3. The workload container is never faulted, so it survives to read the exit code.
