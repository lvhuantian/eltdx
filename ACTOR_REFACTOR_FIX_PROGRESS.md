# Actor Transport Refactor Fix Progress

This temporary ledger supersedes the completion claim in
`ACTOR_REFACTOR_RESULT.md` while the F00-FINAL correction cycle is in progress.
The result document remains historical evidence only until FINAL rewrites it.

## Recovery Identity

| Field | Value |
| --- | --- |
| Recovery date | 2026-07-14 (Asia/Shanghai) |
| Baseline HEAD | `994c49b51f47255bdcd9cdc3308a5a554f37588b` |
| Base | `71089c0a2867a75dc79aa2c340213f4e3845b6e3` |
| Branch | `actor-transport-refactor` |
| Draft PR | [#12](https://github.com/electkismet/eltdx/pull/12), confirmed OPEN and draft at pushed HEAD `2e48be0` |
| Baseline worktree | Clean (`git status --short --branch`) |
| Superseded result | Existing `COMPLETE` claim and 183-test evidence |

The PR state and exact pushed head must be refreshed before each checkpoint and
again before FINAL evidence is accepted.

## Constraints

- Do not merge, modify `main`, tag, release, publish, or change port 7615.
- Do not broaden business APIs or add runtime dependencies.
- Preserve one Actor and one connection per pool slot.
- Append checkpoint commits; do not amend, rebase, or force-push published work.
- Keep the PR draft until the exact FINAL HEAD has green CI and Pages evidence.

## Checkpoints

| Checkpoint | State | Exit evidence |
| --- | --- | --- |
| F00 baseline, review, regression design | COMPLETE (`05a5e9b`) | Ledger and deterministic A-E failing baselines recorded before implementation |
| F01 receive ordering and boundaries | COMPLETE (`8aa089d`) | Sequence boundary, full-send gate, pre-send drain, heartbeat gate, partial-tail handling, and unified receive failure path |
| F02 request identity and build isolation | COMPLETE (`8aa089d`) | Monotonic request ID, exact cancel, request-local build errors, strict deadline, terminal-owned slot, callback isolation |
| F03 connect and failover | COMPLETE (`2e48be0`) | Candidate/attempt budgets, next-endpoint retry, Windows peer verification, non-busy rearm, and seven real/fault-injected regressions |
| F04 Broker and pinned leases | COMPLETE in this checkpoint | Per-waiter Event, exact lease/ticket cancellation, FIFO admission, monotonic pinned close, and capacity preservation |
| F05 lifecycle and shutdown | COMPLETE in this checkpoint | Candidate ownership, epoch guard, submission/retire gate, single shutdown attempt, rollback ownership, and monotonic failed-close states |
| F06 stress, performance, resources, compatibility | IMPLEMENTED; heavy evidence pending | Unique wire nonce/provenance, two servers, warmed exact resource plateau, strict heartbeat gate, full Windows CI scope, and reproducible baseline source root |
| FINAL independent review and CI | PENDING | Two clean adversarial reviews; local matrix/build/docs and exact-HEAD CI/Pages green |

## Confirmed Blockers

### A. Receive and request boundary

- The Actor drains control before `TcpGeneration.decoded_frames`; request B can
  start before frames already decoded for A are classified.
- `_route_frame()` checks only the current message ID/type, so an old colliding
  frame can complete B. A public-pool probe returned stale value `999` even
  though the server received B and deliberately sent no response.
- Handshake completion may start the business exchange inside the same decode
  batch, exposing remaining handshake-batch frames to the new exchange.
- Decoded-queue continuation calls the receive path outside the common
  `ProtocolError`/`OSError`/EOF boundary and can turn a normal EOF into Actor
  fatal after a fairness slice.

### B. Ticket, cancel, frame build, and deadline

- `RequestTicket` has no monotonic request identity. `CancelToken` matches a
  reused lease identity, so a late standalone or pinned cancel can kill a later
  request.
- `build_command_frame()` errors escape the active exchange. A READY runtime
  probe with command `0x9999` failed the Actor; the following valid request then
  raised `ConnectionClosedError`.
- `wait_ticket()` adds 50 ms and the Socket facade may add another 50 ms after
  cancellation. Runtime startup also sits outside some public deadlines.

### C. Connect and endpoint rotation

- `_start_request_attempt()` resets `endpoint_index` to zero after a generation
  is dropped. Handshake, send, EOF, and response retries therefore revisit the
  failed endpoint instead of rotating.
- A two-loopback probe observed two accepts on the handshake-EOF server, zero on
  the healthy server, and no `777` response.
- On Windows, a real closed loopback port followed by a healthy listener used
  the entire 1.001 s timeout and never connected to the healthy endpoint. The
  existing fake `SO_ERROR` test does not cover this behavior.

### D. Broker and pin

- `LeaseBroker.acquire()` reuses a thread-local `Event`; a late `set()` can wake
  the next waiter and leave a ghost waiter/lease (ABA).
- A pinned proxy sets `_closed` before quiescence. If close times out, subsequent
  close calls return immediately and a pool-size-one lease is permanently lost.
- Pinned `connect()` bypasses pin-local admission/active-operation accounting;
  the proxy also omits `last_handshake` and `last_heartbeat` compatibility.

### E. Runtime and pool lifecycle

- `PoolRuntimeGuard.add_runtime()` does not validate fatal state, active epoch,
  or Broker identity; a fatal/old-epoch late runtime can survive registration.
- `SocketTransport.close()` can return while an unpublished startup candidate
  Actor is still alive.
- Concurrent pool closers have no single owner. A second close can overwrite
  `FAILED_CLOSING` with `STOPPED` and clear retained cleanup state.
- Pool `connect()` waits for every future before reacting to the first failure,
  so a failed slot does not promptly stop blocked siblings.

## Historical F06 Evidence Gaps

The following gaps describe the superseded pre-F06 evidence. The current F06
worktree closes the first four in deterministic tests; the final 10k/100k and
exact-source performance artifacts remain to be generated after checkpointing.

- Stress responses are all the constant `23285`; cross-request and
  cross-generation completion are not measurable.
- Stress uses one loopback server, so failover is not exercised.
- Windows resources are sampled once and allow `+24`; there is no warmed,
  repeated monotonic-growth analysis.
- The heartbeat gate permits 5 percent loss rather than requiring under 1
  percent impact.
- Concurrent p50 regression was previously reported as 15.41 percent, over the
  10 percent target.

## Command and Failure Log

| Date | Command/check | Result |
| --- | --- | --- |
| 2026-07-14 | `git status --short --branch` | Clean branch `actor-transport-refactor` |
| 2026-07-14 | `git log -1 --oneline --decorate` | `994c49b Make reconnect verification deterministic` |
| 2026-07-14 | `gh pr view 12 --json ...` | Network timeout to `api.github.com`; no state mutation |
| 2026-07-14 | Three independent read-only A/B, C, and D/E reviews | All reproduced blocking defects; no files changed |
| 2026-07-14 | `python -m pytest -q tests/test_transport_actor_regressions.py` on `994c49b` code | **7 failed in 0.51s**, as required before implementation |
| 2026-07-14 | `python -m pytest -q tests/test_transport_failover_regressions.py` before F03 | **5 failed in 5.13s**, as required before implementation |
| 2026-07-14 | F01/F02 focused Actor/Socket/Pool/Lifecycle set | **65 passed in 3.28s** |
| 2026-07-14 | `python -m pytest -q --ignore=tests/test_transport_failover_regressions.py` | **200 passed in 28.45s**; F03 red tests intentionally remain outside the F01/F02 checkpoint |
| 2026-07-14 | F03 regressions repeated five consecutive rounds | Every round **7 passed**; 11.72 s aggregate latest run |
| 2026-07-14 | Full suite after F03 and two read-only reviews | **207 passed in 32.33s** |
| 2026-07-14 | `python -m pytest -q tests/test_transport_pool_regressions.py tests/test_transport_lifecycle_regressions.py` before F04/F05 | **9 failed, 1 passed in 0.63s**, as required before implementation |
| 2026-07-14 | F04/F05 focused regressions after implementation | **54 passed in 2.03s** |
| 2026-07-14 | Actor/Socket/Pool/lifecycle/resources matrix | **124 passed in 6.87s** |
| 2026-07-14 | First full suite after the final lifecycle changes | **247 passed, 1 failed in 32.11s**; only `test_idle_actor_blocks_and_heartbeat_defers_under_continuous_work`, ratio `0.93921 < 0.95` |
| 2026-07-14 | `run_heartbeat_impact(5000, trials=7)` alternating-order diagnosis | Median ratio **0.981680**; heartbeat samples sent 0/4/0/0/2/0/0 and the two slowest active samples sent zero, showing substantial scheduling variance rather than wire-heartbeat load; strict FINAL 0.99 gate remains open for F06 |
| 2026-07-14 | Three independent F04/F05 adversarial reviews | Clean: F04 Actor/Broker/pin, F05 Socket/guard, and F05 pool shutdown found no remaining correctness blocker |
| 2026-07-14 | `python -m compileall -q src tests` | PASS |
| 2026-07-14 | `python -m pytest -q tests/test_transport_pool_regressions.py tests/test_transport_lifecycle_regressions.py tests/test_transport_pool.py tests/test_resources.py` | **66 passed in 1.77s** |
| 2026-07-14 | Full suite after heartbeat diagnosis, same worktree | **248 passed in 30.11s** |
| 2026-07-14 | Exact `117b8c6` PR CI | Windows 3.11/3.13 and Pages build passed; Ubuntu 3.10-3.13 each failed only two MCP serialization tests because their context manager still made a live network connection |
| 2026-07-14 | Warmed Windows resource probe, 12 rounds x 50 generations | Handle sequence was exactly **168** in every round; Actor threads **0** and process threads **1** after every close/GC |
| 2026-07-14 | Unique two-server generation stress, three 1,000-generation rounds | Every round used both real listeners, retained the exact Runtime/Thread objects, and reported 1,000 unique responses with **0** cross-request/cross-generation completions |
| 2026-07-14 | Partial response followed by EOF baseline | Mixed soak raised non-retryable `ProtocolError: truncated response frame at EOF`; fixed by mapping only decoder-finalization truncation at EOF to retryable `ConnectionClosedError`; real two-loopback regression passes |
| 2026-07-14 | Strict heartbeat diagnostic, 32 balanced phases x 1,000 unique requests | Off/on raw aggregate ratio **1.003240**; four block ratios 1.001236/1.009266/1.001700/1.000797; timed heartbeats **0**; 35,216/35,216 unique completions and both cross counters **0** |
| 2026-07-14 | Direct FIFO handoff performance experiment | Rejected and fully removed: four current p50 samples 149.89/151.92/150.95/149.91ms versus exact `117b8c6` 153.36/151.32/150.97/150.91ms; gain was not stable enough to justify new lifecycle state |
| 2026-07-14 | Fresh detached `71089c0` baseline, pool 4/concurrency 100, four 3,000-request 5ms samples | p50 147.62/150.79/150.03/143.45ms; old one-off 129.97ms result is not reproducible, so FINAL will use exact-source counterbalanced evidence |
| 2026-07-14 | F06 stress/failover/MCP focused matrix | **16 passed in 68.46s** |
| 2026-07-14 | Full suite after F06 implementation | **250 passed in 72.75s** |
| 2026-07-14 | `python -m build` | Built `eltdx-1.0.2.tar.gz` and `eltdx-1.0.2-py3-none-any.whl` |
| 2026-07-14 | `python -m mkdocs build --strict` | PASS in 3.69s |

F03 preserves the public absolute deadline and assigns only private candidate
and first-attempt sub-deadlines. Handshake timeout/EOF, partial business send,
business EOF, and response timeout all continue at the endpoint after the one
that failed. The real Windows reserved-but-not-listening first endpoint reaches
the healthy loopback within the same deadline. An anomalous ready socket whose
`SO_ERROR` remains in progress is unregistered and rearmed on a 10 ms selector
timer; the 20 ms regression observes at most four `SO_ERROR` reads instead of
the reviewed 5,221-call busy loop.

The nine D/E baseline failures cover the delayed-Event ABA wake, pinned close
capacity loss, pinned connect releasing a live slot, missing pinned snapshot
properties, fatal/old-epoch late registration, unpublished candidate cleanup,
concurrent close overwriting `FAILED_CLOSING`, and pool connect waiting for a
blocked sibling before requesting stop.

F04 now gives each admission its own terminal Event and exact reservation,
decrements pin reservations before wakeup, preserves call-order FIFO, and never
releases a pinned lease until its active or pending Connect/Request ticket is
terminal. Notify failure atomically withdraws and terminalizes the exact ticket.

F05 installs each Actor candidate before `Thread.start()`, retains the exact
runtime on startup failure, and serializes pool epoch validation with mailbox
submission. The pool guard is identity-bound and seals an epoch before stop.
Concurrent close and FIRST_EXCEPTION rollback share one `ShutdownAttempt` from
admission retirement through cleanup; an old attempt cannot stop a reopened
broker epoch. Startup cleanup is best-effort per stage and its failure is
consumed by shutdown instead of publishing a false `STOPPED` state.

F06 stress requests now use the existing retry-safe file-content command. A
uint32 request nonce is echoed with the real loopback server ID, connection ID,
and wire-attempt sequence. Two real listeners share a provenance ledger and
inject same-batch future-ID poison frames, partial-frame EOF, reconnects, push,
and concurrency. Artifacts explicitly report unique, duplicate, missing,
unexpected, cross-request, and cross-generation completions. Runtime references
are retained across close to prove the Actor thread, TCP generation, selector,
wakeup pair, pending/active tickets, cancel map, Broker waiters/pin waiters/
leases, and PushBuffer are terminal.

The warmed resource gate runs repeated complete lifecycle rounds after warmup
and requires an exact stable OS-resource plateau instead of an unexplained
tolerance. Heartbeat acceptance first proves all four idle connections complete
automatic heartbeats, then runs paced heartbeat/business rounds and a 5-producer
against 4-slot balanced throughput matrix. Business and heartbeat responses use
the same 5ms server delay; every timed enabled sample must send zero heartbeat
under real Broker pressure and the aggregate throughput ratio must be >0.99.

The seven A/B baseline failures were:

- `test_old_decoded_batch_cannot_complete_next_request_after_64_frame_budget`:
  B returned normally from the queued `999` frame instead of timing out.
- `test_handshake_batch_tail_cannot_complete_business_exchange`: the business
  ticket returned normally from the handshake batch tail instead of timing out.
- `test_late_cancel_of_completed_ticket_does_not_cancel_next_lease_zero_request`:
  B raised `ConnectionClosedError: 7709 request cancelled`.
- All three `test_ready_actor_survives_request_build_errors` cases left the
  runtime in `FAILED` instead of `RUNNING`.
- `test_wait_ticket_uses_only_absolute_deadline_no_fixed_grace` observed a
  `0.25` second Event wait for a `0.20` second absolute deadline.

The five C baseline failures proved that handshake EOF, business EOF, and a
first response timeout never reached the healthy second loopback server; the
all-failed case only exercised the first server; and the real Windows closed
port consumed the full one-second deadline with zero healthy accepts.

Exact pytest node IDs and failure output will be appended before each related
implementation change. Intentional red tests will not be left as the terminal
state of a pushed correction checkpoint.

## Exact Next Action

Create and push the F06 implementation checkpoint. While its exact-head CI and
Pages run, execute the heavy 10,000-generation/100,000-request/two-server stress
artifact and the clean detached `71089c0` versus F06 pool-size-one and
concurrent-100 performance matrix. Then update the permanent result, run two
fresh FINAL adversarial reviews, delete this ledger only after all evidence is
transferred, push FINAL, and wait for exact-FINAL-head CI and Pages success.
