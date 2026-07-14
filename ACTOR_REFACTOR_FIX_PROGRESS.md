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
| Draft PR | [#12](https://github.com/electkismet/eltdx/pull/12), last confirmed OPEN and draft at `994c49b` |
| Baseline worktree | Clean (`git status --short --branch`) |
| Superseded result | Existing `COMPLETE` claim and 183-test evidence |

The GitHub API was unreachable during the latest F00 refresh. The PR state above
is the last successful read; it must be refreshed before each pushed checkpoint.

## Constraints

- Do not merge, modify `main`, tag, release, publish, or change port 7615.
- Do not broaden business APIs or add runtime dependencies.
- Preserve one Actor and one connection per pool slot.
- Append checkpoint commits; do not amend, rebase, or force-push published work.
- Keep the PR draft until the exact FINAL HEAD has green CI and Pages evidence.

## Checkpoints

| Checkpoint | State | Exit evidence |
| --- | --- | --- |
| F00 baseline, review, regression design | IN PROGRESS | Ledger committed; A-C defects reproduced; D-E regression set remains to be landed before implementation |
| F01 receive ordering and boundaries | COMPLETE at next checkpoint | Sequence boundary, full-send gate, pre-send drain, heartbeat gate, partial-tail handling, and unified receive failure path |
| F02 request identity and build isolation | COMPLETE at next checkpoint | Monotonic request ID, exact cancel, request-local build errors, strict deadline, terminal-owned slot, callback isolation |
| F03 connect and failover | COMPLETE at next checkpoint | Candidate/attempt budgets, next-endpoint retry, Windows peer verification, non-busy rearm, and seven real/fault-injected regressions |
| F04 Broker and pinned leases | PENDING | No Event ABA; pin close/connect state is monotonic and capacity is preserved |
| F05 lifecycle and shutdown | PENDING | Late registration, start/close/fatal, and concurrent close are fail-closed and leak-free |
| F06 stress, performance, resources, compatibility | PENDING | Unique response stress, two servers, warmed resources, performance and heartbeat thresholds |
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

## Evidence Gaps to Replace

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

F03 preserves the public absolute deadline and assigns only private candidate
and first-attempt sub-deadlines. Handshake timeout/EOF, partial business send,
business EOF, and response timeout all continue at the endpoint after the one
that failed. The real Windows reserved-but-not-listening first endpoint reaches
the healthy loopback within the same deadline. An anomalous ready socket whose
`SO_ERROR` remains in progress is unregistered and rearmed on a 10 ms selector
timer; the 20 ms regression observes at most four `SO_ERROR` reads instead of
the reviewed 5,221-call busy loop.

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

Finish the F00 deterministic regression set. Start with the greater-than-64
decoded-frame collision where A succeeds, the server receives B but does not
respond, and B must never return stale value `999`. Preserve the baseline failure
output here, then implement F01 and run the focused plus full transport suites.
