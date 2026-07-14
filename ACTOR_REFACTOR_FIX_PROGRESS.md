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
| Draft PR | [#12](https://github.com/electkismet/eltdx/pull/12), confirmed OPEN and draft at pushed HEAD `dcf6190` |
| Final-review correction base | `cc46e6042e60b1d70732ae813b089f9c8b572572` |
| Latest pushed correction checkpoint | `dcf619021771ef7c0592fa46a8313b44f798a2e8`; exact CI and Pages successful |
| Current local follow-up | Exact stress/heartbeat/resource/throughput evidence passes; saturated concurrent p50 gate fails and conflicts with strict FIFO semantics |
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
| F04 Broker and pinned leases | COMPLETE (`117b8c6`, corrections `0b8ad54`, `b66a7a8`) | Per-waiter Event, exact lease/ticket cancellation, FIFO admission, close broadcast to every pin-local waiter, and failed-pin capacity recovery |
| F05 lifecycle and shutdown | COMPLETE (`117b8c6`, corrections `0b8ad54`, `b66a7a8`) | Candidate ownership, epoch guard, single close owner, exact resource retention, best-effort cleanup, and monotonic failed-close states |
| F06 stress, performance, resources, compatibility | ACCEPTANCE OPEN (`dcf6190`) | Exact stress, resources, heartbeat, matrix, both throughput gates, sequential latency, and concurrent p99 pass; concurrent p50 is 31.06 percent over baseline against a 10 percent limit |
| Final-review correctness correction | PUSHED (`b66a7a8`, performance correction `dcf6190`) | DNS/host fallback, startup and socket ownership, Broker/pin races, close-owner cleanup, pool-connect identity, `CommandSpec` compatibility, and Actor hot-path selector state; exact CI/Pages and 299-test local suite passed |
| FINAL independent review and CI | PENDING | Two clean adversarial reviews; local matrix/build/docs and exact-HEAD CI/Pages green |

## Current Acceptance Blocker

The first predeclared exact-`dcf6190` 10,000/100,000 A-B-B-A campaign is
retained as a failure, not rerun until a favorable sample appears. Both
throughput gates pass (sequential 97.27 percent, concurrent 96.58 percent), as
do sequential p50/p99 and concurrent p99. Concurrent p50 does not pass:

- Baseline run-summary median: 116.8734 ms.
- Current run-summary median: 153.1688 ms.
- Delta: +36.2954 ms (31.06 percent).
- Allowed delta: max(11.6873 ms, 0.2 ms); passing ceiling 128.5607 ms.

Strict call-order FIFO is also an explicit design requirement in
`ACTOR_REFACTOR_PLAN.md`. With 100 closed-loop callers and measured throughput
near 645 requests/second, Little's law gives about 155 ms per caller cycle,
which matches the narrow current distribution. Reaching 128.6 ms under FIFO
would require about 778 requests/second; four connections with a fixed 5 ms
server delay have an absolute ceiling of 800 before protocol, loopback, thread,
and timer overhead. The old lock scheduler obtains 93-140 ms p50 through
barging, while other callers absorb 261-549 ms p99. Three independent reviews
conclude that no small Broker or Actor optimization can satisfy both strict
FIFO and this raw saturated p50 comparison. Changing either requirement is a
material specification decision and is not authorized by the current goal.

A post-failure, read-only no-backlog diagnostic was predeclared as one complete
10,000-request A-B-B-A block at pool 4/concurrency 4. It measured baseline versus
current p50 of 5.8856/5.9845 ms and p99 of 7.2848/7.4216 ms: deltas of only
0.0989/0.1369 ms, both below the original 0.2 ms floor. This supports separating
fixed scheduling cost from saturated FIFO queue residence, but it is not an
acceptance artifact and cannot retroactively change the failed `dcf6190`
campaign. Three reviews agree that a prospective replacement protocol must be
authorized before implementation, freeze all samples and stopping rules, retain
the saturated raw p50/p99 disclosure, and use fixed cohorts with raw per-request
latencies to prevent old lock barging from gaming the comparison.

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
| 2026-07-15 | Exact clean `0955a8e` heavy stress command | 10,000 generations and 100,000 mixed requests both used two real servers; 110,000/110,000 unique responses; duplicate/missing/unexpected/cross-request/cross-generation all **0**; exact Actor/TCP/selector/wakeup/ticket/Broker/Push cleanup; heartbeat ratio **0.997404** with 32 paced completions and 0 timed heartbeats; close p99 3.1032/2.7951ms; warmed handles **199 x 8** |
| 2026-07-15 | Heavy artifact evidence audit | Rejected two cold per-function handle pairs (`192 -> 209`, `205 -> 338`): sampling occurred before the worker function frame returned and retained joined Thread objects; the next invocation returned to 168. Removed cold pairs entirely and retained only the outer warmed repeated sampler required by the objective |
| 2026-07-15 | Resource/generation/mixed regressions after sampler correction | **3 passed in 3.97s** |
| 2026-07-15 | Three independent FINAL reviews of exact `cc46e60` | Reopened acceptance: DNS consumed the public deadline; selector/wakeup leaked before publication; Broker could assign after deadline; exported six-argument `CommandSpec` construction broke; RESULT evidence attribution was incomplete |
| 2026-07-15 | Deterministic read-only probes on `cc46e60` | DNS 30 ms preflight with 10 ms timeout failed at Actor startup; selector register failure left `close_calls=0`; Broker release assigned a waiter after its deadline; six-argument `CommandSpec(...)` raised `TypeError` |
| 2026-07-15 | DNS/API compatibility review probes | One failed hostname aborted a healthy numeric fallback and leaked raw `gaierror`; close/reopen could inherit a stale resolver; old public calls could cross close; all corrected with standalone and pooled regressions |
| 2026-07-15 | Broker/pin/pool-connect adversarial probes | Closed Broker could wake an invalid assigned lease; pin handoff had the same delayed-wake and expiry windows; pool `connect()` bypassed Broker admission and a stale connect gate could bleed into a reopened epoch |
| 2026-07-15 | Actor cleanup adversarial probes | Resources are published immediately; teardown is best-effort per stage; a close failure retains the exact selector and `cleanup_error`; public close raises instead of claiming resource-free success |
| 2026-07-15 | `python -m compileall -q src tests scripts` after final-review corrections | PASS |
| 2026-07-15 | Protocol/Actor/Socket/Pool/lifecycle focused matrix | **122 passed in 2.56s** |
| 2026-07-15 | New DNS/cleanup/Broker/pin/pool-connect node regressions | All targeted nodes passed, including delayed Event close races, expired handoffs, all-slot connect admission, old/new Broker identity, resolver generations, and post-close heartbeat stability |
| 2026-07-15 | `python -m pytest -q` after final-review corrections | **270 passed in 74.06s** |
| 2026-07-15 | Independent correctness re-review of current correction worktree | CLEAN for code: reproduced races closed; compile/diff checks passed; result/evidence still pending exact new SHA |
| 2026-07-15 | Exact pushed `0b8ad54` PR checks | CI run `29357246798` passed Ubuntu 3.10-3.13 and Windows 3.11/3.13; Pages run `29357250104` passed |
| 2026-07-15 | Post-`0b8ad54` adversarial red probes | Reproduced pin-local waiter stranding, assigned-before-wire FAILED lease loss, invalid numeric-port Actor fatal, facade/unpublished Push cleanup loss, callback/selector cleanup suppression, snapshot/notify/startup-wait close-owner escapes, and late-candidate stop loss |
| 2026-07-15 | Post-review deterministic regression nodes after fixes | Pin/host/cleanup/owner exact nodes all passed; full Actor/Socket/Pool/lifecycle/resources matrix **164 passed in 7.26s** |
| 2026-07-15 | Cleanup/close-owner independent re-review | CLEAN: exact owned buffers, deferred non-Push errors, multi-runtime best-effort stop/close, late candidates, and every owner exception boundary re-probed |
| 2026-07-15 | Pin/host independent re-review | CLEAN: 600 reserve/close and release/close race rounds, exact Event unregister, FAILED lease auto-release, FIFO, ports 0/1/65535/65536/99999, mixed resolve, and bracketed IPv6 probe |
| 2026-07-15 | First expanded full suite | **295 passed, 1 failed**; only strict heartbeat aggregate ratio `0.987068 < 0.99`, with zero timed wire heartbeats; acceptance remained open |
| 2026-07-15 | Heartbeat measurement and production correction | Counterbalanced odd/even phase positions; active business now exits `_schedule_heartbeat` before heartbeat-only reads; aggregate elapsed ratio remains the strict `>0.99` gate and block median is diagnostic only |
| 2026-07-15 | Strict heartbeat test after hot-path correction | PASS; independent aggregate sample `1.000357`, zero timed wire heartbeats |
| 2026-07-15 | `python -m compileall -q src tests scripts` and `git diff --check` | PASS |
| 2026-07-15 | Latest `python -m pytest -q` | **296 passed in 71.29s** |
| 2026-07-15 | Exact pushed `b66a7a8` PR checks | CI run `29363110965` passed Ubuntu 3.10-3.13 and Windows 3.11/3.13; Pages run `29363110985` passed |
| 2026-07-15 | Clean exact-`b66a7a8` heavy stress | 10,000 generations and 100,000 mixed requests completed with both servers used; 110,000 unique results and all duplicate/missing/unexpected/cross counters **0**; close resources terminal; close p99 2.9312/2.6155 ms; warmed handles exactly **188 x 8**; artifact SHA256 `316D0C39001410FB513FC52AEA08A53B50A8C10B0BB90468205E84702EEC6974` |
| 2026-07-15 | Three predeclared heartbeat repetitions at `b66a7a8` | Ratios 0.995257/1.000617/0.996396; combined 96 timed phases and 105,696 business responses gave ratio **0.997418**, zero timed heartbeats and zero cross counters |
| 2026-07-15 | Exact-`b66a7a8` full 1/2/4 x 1/10/100 matrix | Pool 1 sustained about 163 rps, pool 2 about 321 rps, and pool 4 about 642 rps with observed maximum active work exactly 1/2/4; artifact SHA256 `1DFBDB75F22631F9CB55BD93287ED5D124A08D3EFC747450CE7524F62374C92B` |
| 2026-07-15 | Full 10,000 sequential / 100,000 concurrent baseline-current-current-baseline acceptance at `b66a7a8` | Sequential ratio **0.948497**, concurrent ratio **0.943353**; both below required 0.95, so FINAL remained open |
| 2026-07-15 | Actor hot-path correction verification | Normal immediate sends retain READ interest without selector modify; partial/would-block sends arm READ|WRITE; pooled/pinned calls pass their exact completion without a redundant lock wrapper. Compile passed, focused matrix **162 passed**, full suite **299 passed in 69.93s** |
| 2026-07-15 | Counterbalanced hot-path diagnostics against clean `b66a7a8` | Sequential 3,000-request ABBA measured **+0.3751%**. Two independent 4 x 10,000 concurrent ABBA sets combined to about **+2.0%** but showed system drift, so they are diagnostic only. A stale-wakeup drain experiment measured about **-0.56%** and was rejected without source changes |
| 2026-07-15 | Exact pushed `dcf6190` checks | CI run `29368040831` passed Ubuntu 3.10-3.13 and Windows 3.11/3.13; Pages run `29368040880` passed |
| 2026-07-15 | Clean exact-`dcf6190` heavy stress | 10,000 generations in 17.130445s and 100,000 mixed requests in 72.418717s used both servers; 110,000 unique results and all duplicate/missing/unexpected/cross counters **0**; 800 close cleanup snapshots terminal; close p99 3.1632/2.7634 ms; handles exactly **189 x 8**; heartbeat ratio 1.011021; artifact SHA256 `4180E3F50C24BB950F341B32D9559892080DDC3B5A5300DA9FAE64A6F42678EC` |
| 2026-07-15 | Three fixed exact-`dcf6190` heartbeat repetitions | Ratios **0.994514/0.992093/0.999226**; combined raw elapsed ratio **0.995277** across 105,696 unique business responses, zero timed heartbeats and zero cross counters; artifact hashes `6B4A56E0198471CCF907AB28049521FBF8865929D099D392B6A9D889E476410C`, `D07819BF318834E62D1378838D5065076450A1AC9505A662572102E253050D69`, `AD7CE4B3C6DA153E1FDF3041D18D73AE3DEED5F25D764DE6496C1B4FE02F366F` |
| 2026-07-15 | Exact-`dcf6190` full 1/2/4 x 1/10/100 matrix | All nine 3,000-request cases completed with maximum active work 1/2/4; artifact SHA256 `2CCFC7C56852A19AC3DDE5F91249E3C161E08C813C618C7C49DED2DECD0CFBC6` |
| 2026-07-15 | First exact-`dcf6190` full A-B-B-A acceptance | Workload SHA `9E7A7FB2E7DA00DA86B956AE8081575C35F53A3E243292FB05FA0FC83B672338`; throughput PASS at 97.27/96.58 percent; sequential p50/p99 and concurrent p99 PASS; concurrent p50 **FAIL** at 153.1688 ms versus 116.8734 ms baseline and 128.5607 ms ceiling. Artifact SHA256 values in A1/B1/B2/A2 order: `7E51D20A5412DB2BD855521C145A75EDF897F8F3F829788E55B8883FF60AB444`, `F1D71435F0ACD4B7E81916DE9C59EF1F336BC0A50D4752C98309078BF303DE86`, `AF52F0C06CAC877AFD0DCA6FCC101DA5964CA2300CF53228727A860135EF6D60`, `BA2CC234DEB9E165C23DCD49D4381E14ED4AA113D8E8D4676176640B17063D84` |
| 2026-07-15 | Three independent exact-performance audits | All classify the campaign overall **FAIL**. Strict FIFO gives p50 approximately `N/X`; direct handoff already exists, batch wake cannot create leases, earlier release would violate one-inflight ownership, and matching the raw baseline p50 would require barging or a material specification change |
| 2026-07-15 | Post-failure no-backlog latency diagnostic | One predeclared pool 4/concurrency 4, 10,000-request A-B-B-A block retained all four runs. Baseline/current run-summary p50 was **5.8856/5.9845 ms** and p99 **7.2848/7.4216 ms**; deltas 0.0989/0.1369 ms are below 0.2 ms. Diagnostic only: it does not replace or reclassify the failed saturated campaign |

Post-`0b8ad54` corrections make Broker close broadcast every independently
registered pin waiter Event without retaining proxies. A delayed assigned caller
that never creates `PinCompletion` now releases a FAILED exact lease itself.
Socket close owns every facade/runtime/candidate PushBuffer identity, preserves
the first deferred non-Push cleanup error, and guards every exception after the
close-owner bit is published. Invalid ports are rejected before DNS/Actor work,
and bracketed IPv6 probe addresses are canonicalized consistently with resolve.
The stress schema now binds close snapshots to the exact returned ticket and
resources, records configured/high-water Push limits and endpoint provenance,
and uses token-bound keep-open ownership.

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

## Final-Review Correction Scope

The exact `cc46e60` implementation and its artifacts are historical correction
evidence, not the FINAL implementation. FINAL review found and the current
worktree corrects all of the following without changing business command
methods, port 7615, pool-size meaning, or runtime dependencies:

- Standalone hostname resolution is a caller-side preflight before deadline,
  Actor, TCP, ticket, or lease creation. Concurrent callers share only an exact
  `(close_generation, epoch)` resolver claim; a reopened generation can resolve
  independently and stale results cannot publish or clear the new claim.
- Per-host DNS failure is isolated so a healthy later host remains usable; only
  an all-failed set raises `ConnectionClosedError` in standalone and pool paths.
- Actor selector and wakeup resources are published as soon as created.
  Teardown attempts every stage, retains any resource whose close raised, stores
  the first `cleanup_error`, and makes public close fail closed instead of
  reporting successful cleanup.
- Broker and pin-local assignment check absolute deadlines while holding the
  scheduling lock. Delayed assignment wakeups revalidate exact live identity
  after close, expired FIFO entries are terminalized before later live entries,
  and pin reservations are released before wakeup.
- Explicit pool `connect()` holds all N exact Broker leases, so concurrent
  execute/pin work cannot enter those slots. Connect serialization is bound to
  Broker identity; a stale old-epoch attempt neither blocks nor clears a new one.
- The appended public `CommandSpec.retry_safe` field defaults to conservative
  `False`; all built-in commands remain explicitly `True`.

The stress harness at `b66a7a8` now enforces non-vacuous saved-resource
presence, close-future terminal checks, per-request cross-endpoint retry
accounting, and explicit PushBuffer capacity/high-water fields. The production
hot-path correction invalidates that checkpoint as FINAL evidence, so all
artifacts must be regenerated at the next exact implementation SHA.

## Exact Next Action

Record and push this exact evidence failure without the unfinished RESULT
rewrite. Completion then requires an explicit decision between the two
conflicting requirements: preserve strict call-order FIFO and revise the raw
saturated p50 acceptance definition prospectively, or preserve the raw p50
comparison and authorize a non-FIFO/barging scheduler. Do not silently change
the benchmark, discard this failed campaign, relax FIFO, or declare FINAL.
After that decision, implement the selected contract, freeze the new acceptance
campaign before its first sample, regenerate exact-SHA evidence, transfer it to
RESULT, run at least two new clean FINAL reviews, delete this ledger, commit
FINAL with both required trailers, push, and wait for exact FINAL-head CI and
Pages success.
