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
| Draft PR | [#12](https://github.com/electkismet/eltdx/pull/12), confirmed OPEN and draft at pushed HEAD `2da76518a785c6c167474b9826863c1d3cf98953` |
| Final-review correction base | `cc46e6042e60b1d70732ae813b089f9c8b572572` |
| Latest pushed correction checkpoint | `e106ad484ff90aa5602a24926ca527486197fda9`; exact CI run `29429941357` and Pages run `29429941406` passed |
| Current local follow-up | Formal `fifo-v2-2da7651-a` remains a permanent FAIL. Windows Actor cooperation is checkpointed and exact remote checks are green; one remote-evidence ledger commit precedes the next new declaration |
| Baseline worktree | User-owned modification in `ACTOR_REFACTOR_RESULT.md`; preserve and integrate, do not overwrite |
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
| F01 receive ordering and boundaries | CORRECTNESS CLOSED; CHECKPOINT CANDIDATE | READ-first batches, immutable exchange identity, fixed-key collision regressions, and keyed nonrepeating nonzero msg IDs pass; protocol-compliant peer boundary is explicit |
| F02 request identity and build isolation | CORRECTNESS CLOSED; CHECKPOINT CANDIDATE | Exact cancel, terminal claim, FIFO request/control/submission identity gates, physical-lock interruption recovery, and build isolation pass |
| F03 connect and failover | COMPLETE (`2e48be0`) | Candidate/attempt budgets, next-endpoint retry, Windows peer verification, non-busy rearm, and seven real/fault-injected regressions |
| F04 Broker and pinned leases | CORRECTNESS CLOSED; CHECKPOINT CANDIDATE | BaseException-safe waiter withdrawal, assigned-waiter lazy reap, pin close lease recovery, atomic batch admission, and FIFO pass |
| F05 lifecycle and shutdown | CORRECTNESS CLOSED; CHECKPOINT CANDIDATE | Tokenized lifecycle gates, nonblocking finalizers, deadline-bounded best-effort fatal cleanup, and monotonic shutdown pass |
| F06 stress, performance, resources, compatibility | WINDOWS ACTOR COOPERATION CHECKPOINT (`e106ad4`); FIFO-v2 campaign FAIL | Pool-size-specific yield/Event grace passes 485 local tests, builds/docs, two clean post-fix reviews, directionally clean C/E/E/C, and exact CI/Pages; a new one-shot formal campaign is required |
| Final-review correctness correction | COMPLETE (`a53cc09`) | 443-test correctness snapshot plus deterministic two-endpoint generation failover; exact CI and Pages passed |
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

## Authorized Resolution

The user selected option 1 after the exact failure and independent audits:
strict call-order FIFO remains authoritative. Plan revision 1.1 prospectively
separates saturated closed-loop queue residence from added scheduling cost.
FIFO-v1 freezes eight trials in `ABBA + BAAB` order, retains saturated
pool-4/concurrency-100 throughput and raw p50/p99, and independently gates
sequential plus four-worker no-backlog cohort p50/p99 from pooled raw nanosecond
samples. The 100-worker closed wave remains report-only evidence. The old
`dcf6190` campaign remains FAIL.

### FIFO-v1 Campaign A Failure

The first prospective campaign `fifo-v1-ca43972-a` was declared before samples
with canonical SHA256
`47221CD9611C91B43C11F64651A675BF3A7CCFC9063D7686356E8098CC50915A`.
All eight attempt-1 trials completed in exact `ABBA + BAAB` order. The verifier
found no structural, identity, timing, schema, physical-consistency, cleanup,
or sample-count errors. Sequential/saturated throughput ratios were
0.965145/0.954134; sequential p50/p99 and no-backlog p50 passed. No-backlog p99
was 7.0652 ms baseline versus 7.9049 ms current: delta 0.8397 ms against a
0.70652 ms allowance, exceeding the ceiling by 0.13318 ms. Bundle SHA256
`C9F75FBC72B69AA26307EDCF967FA369914F266676374158453C795D62AAEAEA` is
retained as FAIL. The same exact source will not be sampled again.

### FIFO-v2 Prospective Protocol

FIFO-v2 supersedes only the prospective producer/verifier format; it does not
rerun, modify, or reinterpret `fifo-v1-ca43972-a`. Bundle and trial schemas are
version 4. The shared `TYPE_FILE_CONTENT` workload works on both clean
`71089c0` and current source. Setup (`0xE0000000...`), warmup
(`0xC0000000...`), and measured (`0..requests-1`) token domains are disjoint.

Every timed completion retains nine uint32 values: requested, snapshot, and
echoed token; response epoch, connection, and attempt; and the server ledger's
expected epoch, connection, and attempt. The verifier reconstructs token
ranges, uniqueness, duplicate/missing/unexpected counts, cross-request and
cross-generation counts, and the 9xuint32 SHA256 from those rows. It also
requires epoch 1, exact attempt permutation, every pool connection, sequential
attempt order, fixed-cohort wave membership, and physical request/attempt/
success equalities.

Production verification binds both declared roots to existing clean Git
worktrees at the declared SHA, and requires the current root to own the adjacent
producer/verifier. Trial label/id, physical duration lower bounds, and exact
per-case replay across roles are enforced. The frozen declaration states the
achievable artifact trust boundary from plan revision 1.1: local producer raw
measurements are trusted; the verifier detects exact replay and structural or
physical inconsistency, but does not claim cryptographic authentication against
deliberate self-consistent fabrication.

### FIFO-v2 Campaign A Failure

The first schema-4 campaign `fifo-v2-72ef660-a` was declared before any
sample with canonical SHA256
`fafe65ea46f182cdc06d887043758a2c6e6a72c4c7a68de30853a111a3877f39`.
All eight attempt-1 trials completed once in exact `ABBA + BAAB` order. The
verifier returned no evidence errors. Across 32 cases and 1,000,000 raw
completion rows, requests, server requests, attempts, and unique responses were
all exactly 1,000,000; duplicate, missing, unexpected, cross-request, and
cross-generation counts were all zero.

Five hard gates failed:

- Sequential throughput was 173.853591 versus 163.026466 requests/second,
  ratio 0.937723 against the 0.95 minimum.
- Saturated throughput was 680.884296 versus 605.317634 requests/second,
  ratio 0.889017.
- Sequential p99 was 6.2537 versus 7.0782 ms, delta 0.8245 ms against a
  0.62537 ms allowance.
- No-backlog p50 was 6.1475 versus 7.1239 ms, delta 0.9764 ms against a
  0.61475 ms allowance.
- No-backlog p99 was 7.3065 versus 9.0527 ms, delta 1.7462 ms against a
  0.73065 ms allowance.

Sequential p50 passed. Saturated raw latency remained report-only: baseline
p50/p99 108.8690/527.3172 ms versus current 163.62815/183.3818 ms. The failure
was present in both counterbalanced blocks and is not a single-trial outlier.
Bundle SHA256 is
`a533a286a8c3f988b32dd1303ef174a8cbd18067095a92909a0587fc6c79c29d`;
verification report SHA256 is
`5dc53c3c336c417a7a6d610afd4df6929f8461ab43475f01e0e082778376df0b`.
Trial hashes in schedule order are `6045b8ebcbece31695180de62f876235466fdf5ccd773b96bc453d5ad8e5c293`,
`6c3fbb525ac9829a66517df52e08c65adff670816fb0308474e407202f2d6d25`,
`d9d107dc88e289d0371203e7df8809766dc0e1b081f2a828c11f3e5d855bd29f`,
`dd3128fb7cc489bc652c0199b8e2a40c893b1cd724fbdf6898e2fb4b9e670098`,
`a4a7264938c6ff8102055391d65906f6814b9174171a3f9f36ca2dc3b1bac436`,
`e627d0ff9e16f8d96c8867e947cf855f0a386d332c59bf54a0033e9f9efb44e1`,
`cd042c107d6fc750f4c33d3b83cb4b6abcec1332134fd7a03ec0c5ab420edd3a`,
and `29adc67c1470ad1a77c11a3a095ec40dfc9d7db252e053ef8d03784d2d0fb564`.
This exact source and campaign will not be sampled again.

### FIFO-v2 `2da7651` Campaign Failure

The post-hot-path campaign `fifo-v2-2da7651-a` was declared before sampling
against clean baseline `71089c0a2867a75dc79aa2c340213f4e3845b6e3` and
current `2da76518a785c6c167474b9826863c1d3cf98953` worktrees. Its canonical
declaration SHA256 is
`b8276d380998bfc819ca9fb58b36e429e37eb48a3a35ac5e12ea4528538ced62`;
the declaration file SHA256 is
`74162f5585120b226db4932cf7c334c35db915f214bb550ffe5f2fb3a5de9b44`.
All eight attempt-1 trials ran exactly once in fixed
`baseline/current/current/baseline/current/baseline/baseline/current` order.
No cell was retried. The verifier reported `errors=[]`.

Across 32 cases and 1,000,000 completion rows, client successes, server
requests, wire attempts, and unique responses were all exactly 1,000,000.
Duplicate, missing, unexpected, cross-request, and cross-generation counts
were all zero. The immutable campaign bundle SHA256 is
`2d10ed3b55c6aded7146cc699ac344753d11444d05496c07658b48fc3a95ee3c`;
the verification report file SHA256 is
`cc8cd9445d9ed2f263d2ffed1afd783b5e72edddd5a2e3ae02230d55ddedc0e1`.

Four hard gates failed:

- Sequential throughput was 169.640752 versus 160.834299 requests/second,
  ratio 0.948088 against the 0.95 minimum.
- Saturated throughput was 665.262613 versus 625.189760 requests/second,
  ratio 0.939764.
- No-backlog p50 was 6.25775 versus 6.9896 ms, delta 0.73185 ms against a
  0.625775 ms allowance; the passing ceiling was exceeded by 0.106075 ms.
- No-backlog p99 was 7.1636 versus 8.4525 ms, delta 1.2889 ms against a
  0.71636 ms allowance; the passing ceiling was exceeded by 0.57254 ms.

Sequential p50 and p99 passed. Saturated raw latency remained report-only:
baseline p50/p99 115.97525/522.2971 ms versus current
157.0036/176.5262 ms. ABBA throughput ratios were 0.957255 sequential and
0.960391 saturated, both passing; BAAB ratios were 0.939196 and 0.919921,
both failing. No-backlog p50/p99 failed independently in both blocks: ABBA
deltas were 0.76525/1.3198 ms and BAAB deltas were 0.69745/1.2488 ms.
The best current no-backlog p99 trial was 8.0137 ms, still above the worst
baseline trial's 10-percent ceiling of 7.99953 ms. This campaign is permanently
retained as FAIL and this exact source will not be sampled again.

## Post-Campaign Adversarial Reopening At `8303405`

Fresh read-only review after recovery found deterministic gaps that the 340-test
suite did not cover. Correctness is reopened before profiling or changing the
performance hot path:

- A combined selector `READ | WRITE` event completes a partial send before
  receiving data that was already readable. A colliding frame can therefore be
  routed using the later mutable `tx_offset` and complete the request even
  though the frame crossed the wire boundary before full send.
- Cancelling a `RequestTicket` while its generation is `CONNECTING` completes
  the ticket but leaves the connecting generation without an active owner or a
  selector deadline.
- A caller exception while `SocketTransport._connect_with_deadline()` waits does
  not cancel its exact `ConnectTicket`, retaining the slot/request lock until an
  unrelated terminal event.
- `_drain_control()` snapshots cancels and promotes pending work in separate
  critical sections. A cancel linearized between those sections can still let
  the exact pending request start network work before the next control drain.
- The first pin-local call can return from `_admit()`, lose a race to `close()`,
  and still enter the slot while the proxy is `CLOSING`.
- Pool shutdown and connect rollback call the slot retirement path while it can
  block indefinitely on `_submission_gate`; stop is delayed and the nominal
  one-second pool close deadline is not enforced.
- `pool.connect()` acquires N leases one at a time. A later single request can
  take a remaining idle slot between those calls instead of queuing behind the
  all-slot connect admission.

The Windows refused-first loopback regression also needs to assert that the
failed endpoint created the first generation; success on the healthy endpoint
alone would not detect an implementation that skipped endpoint zero.

No production code is changed until deterministic red regressions for these
findings are retained in the worktree and recorded below.

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
| 2026-07-15 | User contract decision | Selected option 1: preserve strict FIFO and prospectively replace the raw saturated p50 hard gate with independently verified fixed-cohort scheduling gates; saturated raw p50/p99 remain mandatory disclosure |
| 2026-07-15 | FIFO-v1 producer/verifier implementation | Added raw `perf_counter_ns` samples, prestarted fixed cohorts, Broker boundary cleanup checks, a two-stage canonical declaration/externally recorded hash, fixed `ABBA + BAAB` schedule, exact identity/config/schema/time checks, pooled integer throughput math, four independent latency gates, and report-only saturated/100-worker raw latency; initial focused evidence tests **20 passed** before adversarial expansion |
| 2026-07-15 | FIFO-v1 adversarial evidence hardening | Closed producer/workload root forgery, impossible elapsed/latency integrals, partial-submit Future loss, double cohort waits, Windows server/accept cleanup, warmup counter contamination, reset/sendall race, policy/stopping rewrites, bool-as-int schema values, unknown hidden fields, derived summary forgery, overlapping/naive timestamps, non-Windows execution, declaration/run hash separation, and terminal failure recording |
| 2026-07-15 | Frozen FIFO-v1 clean-checkout identities before sampling | Exact detached Windows checkout at `11be931` is clean. Plan revision 1.1 SHA256 `716F423F1E10DCF22308970602640502630DE3F2886FAD8DD6BDACB7304B17F5`; producer/workload SHA256 `11FA10B50F857E427FEC75DF7FD913D1CBEA1CBD489CADBFC186FDF6112B3946`; verifier SHA256 `675BBED1D66E4BD7B2DAC0CDBE6EC6BA438A5963D25837D3C71CFBD16815DB13`. Earlier authoring-worktree byte hashes used LF endings and are superseded; Git content was unchanged and no formal sample had started |
| 2026-07-15 | FIFO-v1 focused and full local verification | Evidence suite **41 passed**; complete suite **340 passed in 70.95s**; compileall and diff check passed; wheel/sdist built; MkDocs strict passed |
| 2026-07-15 | Three independent pre-sampling reviews | CLEAN: producer concurrency/cleanup/timing, verifier/runner mathematics and anti-forgery, and user-contract/scope review; no formal FIFO-v1 sample existed before the freeze checkpoint |
| 2026-07-15 | Exact `ca43972` checks | CI run `29375304125` passed Ubuntu 3.10-3.13 and Windows 3.11/3.13; Pages run `29375303924` passed |
| 2026-07-15 | Clean exact-`ca43972` heavy stress | 10,000 generations in 15.839579s and 100,000 mixed requests in 61.194887s used both servers; 110,000 unique results and all duplicate/missing/unexpected/cross counters **0**; 800 close cleanup snapshots terminal; close p99 2.3739/2.4516 ms; handles exactly **189 x 8**; heartbeat ratio 0.999786; artifact SHA256 `309D0B313525A13255279430E1E45C8B34483B8777F1A15AB0C13CCA2210C40B` |
| 2026-07-15 | Three fixed exact-`ca43972` heartbeat repetitions | Ratios **1.002814/1.011513/0.997753**; combined raw elapsed ratio **1.003991** across 105,696 unique business responses, zero timed heartbeats and zero cross counters; hashes `94AE44CD985DB9CDF9FBD7F3FB126992E76606B20F10AD384BB93116465D3230`, `41A77A8D1669BBE2F034C6FBCCF843CD670B2A2D494F13CE0FAD8520798A2DC6`, `EB78B8F95CD2FEF8D6B42F062D444A190C67FBD0AEACA8941A92ADC4AA192EEB` |
| 2026-07-15 | Exact-`ca43972` full 1/2/4 x 1/10/100 matrix | All nine cases returned exactly 3,000 timed server requests; pool4/c100 reached 657.34 rps, p50/p99 150.98/157.25 ms, max active 4; artifact SHA256 `3A43D07E3A1724458CF0CB0027A84B5C87A227BC604CB4FDBAEC32253A8D5D46` |
| 2026-07-15 | FIFO-v1 campaign A declaration and run | Declaration file SHA256 `E66FAB4F1576B801F1D95CCA075ACF754A94C17D5B3F4B469EE5C85DBE877A7A`, canonical SHA256 `47221CD9611C91B43C11F64651A675BF3A7CCFC9063D7686356E8098CC50915A`; eight trials completed once in frozen order; bundle SHA256 `C9F75FBC72B69AA26307EDCF967FA369914F266676374158453C795D62AAEAEA`; verification report SHA256 `04DDB7208B067AED37DC6A884BD1867123455064791F3A7FCDEF298EE413DCFC`; overall **FAIL** only on no-backlog p99 by 0.13318 ms |
| 2026-07-15 | FIFO-v1 campaign A trial hashes in order | `565177A7899AF2C27D092519973AE2098DAE06AA2D190CAD9096CAEFB12FDCB7`, `115AC79700C1A3AEEC3392EA2A609461884BF098BBD853C99219357F80D41E00`, `3E8B1FD88F1A5F60A7A6B5C2E55137ADDB045600FAC90A7A1772C88AE65C17D0`, `7B7A3ED6A3B5C1AEE37E5E6AB61FFB08B8831A9C5116810DEC72BD0A845B6348`, `553201B58379E1EBBC48A8D16282101D2867ABE36D8ADB1CB978907E559BE2EC`, `EF92985718078A60082173EE0B90E1FC6F15F616145D31AFEB1612344DEBCDCA`, `5E7D96F021FCED56F6F2CB5FDDBF2FDF9341BC3813C22F5149BFF956131A02A5`, `2B984DB208C22DC01EC67F719592AA356A8387B3FAA382126F0788899D127F5E` |
| 2026-07-15 | Recovery identity and PR refresh | Local/remote/PR head `83034059d0baa759a1e90b1752626b315f5d907f`; PR OPEN and draft; exact Ubuntu 3.10-3.13, Windows 3.11/3.13, and Pages checks successful; user-owned `ACTOR_REFACTOR_RESULT.md` diff preserved |
| 2026-07-15 | Recovery `python -m pytest -q` | **340 passed in 72.36s** on Windows CPython 3.12.6; this does not supersede the failed FIFO-v1 campaign |
| 2026-07-15 | Three fresh read-only recovery reviews | A/B and pin correctness reopened by deterministic in-memory races; test/CI evidence review confirmed the campaign FAIL and identified a refused-first assertion gap |
| 2026-07-15 | Post-campaign deterministic red matrix: `python -m pytest -q tests/test_transport_actor_regressions.py tests/test_transport_pool_regressions.py tests/test_transport_lifecycle_regressions.py tests/test_transport_failover_regressions.py --tb=short` | **10 failed, 90 passed in 9.01s**. Exact failures: two pre-send receive boundary nodes, RequestTicket CONNECTING cancel, cancel-during-control promotion, interrupted connect exact cancel, atomic batch admission, pin connect/execute after close, pool close submission-gate deadline, and rollback stop-before-retire |
| 2026-07-15 | First post-campaign implementation focused matrix | **150 passed in 13.12s** across Actor/Pool/Lifecycle/Failover regression files |
| 2026-07-15 | First expanded full suite on the uncommitted worktree | **397 passed, 3 failed in 83.38s**. Deterministic regressions: ticket completion published before lease callback and pool fatal hidden as CLOSING; both corrected. Heartbeat ratio 0.984775 remains retained as a failed sample, not erased by a later pass |
| 2026-07-15 | Correctness matrix after the two full-suite corrections | **234 passed in 14.21s** across Actor/Socket/Pool/Lifecycle/Failover/Resources; isolated strict heartbeat node subsequently passed in 60.96s, but formal heartbeat evidence remains pending |
| 2026-07-15 | Second adversarial deterministic red matrix | **7 failed in 1.74s**. Exact failures: connect/request gate acquired-before-owner-publication interrupt (2), Broker admission wait interrupt, pin-local admission wait interrupt, pin close lock-timeout followed by wire terminal lease loss, runtime fatal cleanup short-circuit, and abandon/finalizer cleanup short-circuit |
| 2026-07-15 | Second adversarial owner-publication extension | **5 failed in 0.66s**. Exact failures: connect/request submission-gate acquire-return interrupt (2), Actor control-gate acquire-return interrupt (2), and request build failure clearing active ownership before terminal completion |
| 2026-07-15 | Receive-identity threat-model regression | **1 failed in 0.54s** because ActorRuntime had no keyed, nonrepeating message-token identity; monotonic `msg_id + 1` remained predictable despite receive/exchange batch boundaries |
| 2026-07-15 | Handshake semantic failover regression | **1 failed in 0.53s**: a structurally complete one-byte handshake payload raised non-retryable `ProtocolError`; the healthy second loopback endpoint received zero connections |
| 2026-07-15 | Second red-set implementation verification | **12 passed in 0.45s** for request/submission/control identity, waiter interruption, pin lease recovery, and fatal/finalizer best-effort nodes |
| 2026-07-15 | Third adversarial extensions before implementation | Deterministic red probes retained for physical Condition acquire interruption, pending-cancel mailbox ownership, assigned pin waiter cleanup timeout, finalizer lock contention, reserved msg_id zero, lifecycle direct-gate interruption, and handshake semantic failover |
| 2026-07-15 | Latest four-regression matrix | **172 passed in 13.63s** |
| 2026-07-15 | Latest Actor/Socket/Pool/Lifecycle/Resources/performance-evidence correctness matrix | **297 passed in 14.82s** |
| 2026-07-15 | Latest `python -m pytest -q` | **422 passed in 79.05s** on Windows CPython 3.12.6 |
| 2026-07-15 | Stress poison contract after keyed IDs | Current harness uses same-batch duplicate-response poison; exact future-ID collision is retained in fixed-key deterministic Actor regressions. Historical sequential-ID artifacts remain unchanged |
| 2026-07-15 | Identity-gate adversarial red evidence | Broadcast waiter Events reproduced FIFO inversion and concurrent stale-release ABA (**4 failed**); direct handoff then reproduced expired-waiter grant, `Event.set()` owner stranding, and physical state-lock acquire leakage (**6 failed**); Condition release-after-success reproduced lost compat owner/orphan waiter (**4 failed**); Condition contention reproduced blocked exact release and unpublished terminal completion (**3 failed**) |
| 2026-07-15 | Identity-gate final verification | Shared Actor/Socket gate uses FIFO direct handoff, exact-token state locking, waiter deadline/granted/terminal identity, wake-failure revocation, and Condition-independent release; **23 focused nodes passed**. Ten pre-final rounds were stable, and independent pressure ran IdentityGate and `_RequestGate` for **25 x 800 attempts each** with unique owner and empty terminal state |
| 2026-07-15 | Heartbeat development samples on correction worktree | Two retained strict-gate failures measured **0.989967** and **0.989338** with zero timed heartbeats/cross counters; one raw 32-phase run then measured **0.996441**, and the final isolated node passed in **60.98s** after removing Condition from release. These development samples are not exact-checkpoint F06 artifacts |
| 2026-07-15 | Final local correctness matrices before checkpoint | Four regression files **193 passed in 14.07s**; expanded Actor/Socket/Pool/Lifecycle/Resources/evidence matrix **318 passed in 15.68s**; complete suite **443 passed in 80.33s** on Windows CPython 3.12.6 |
| 2026-07-15 | Final local static/process checks before checkpoint | `python -m compileall -q src tests scripts` passed; `git diff --check` passed with only existing LF/CRLF warnings; no background pytest remained |
| 2026-07-15 | Final post-fix read-only reviews | Protocol review **CLEAN** after all Condition/State/Event before/after fault injections, FIFO/ABA/deadline probes, and 40,000 aggregate gate attempts; concurrency/lifecycle review **CLEAN** after terminal publication under Condition contention, five lifecycle nodes, exact cancel recovery, and gate-focused regressions |
| 2026-07-15 | Exact `7b961fe` remote checks | Pages run `29402921816` passed. CI run `29402921733` passed Ubuntu 3.10/3.12/3.13 and Windows 3.11/3.13, but Ubuntu 3.11 failed `test_one_thousand_generation_changes_keep_one_actor_and_no_resources`: endpoint 1 had zero business requests; package build was skipped. The failure is retained and not rerun as a favorable sample |
| 2026-07-15 | Generation-stress CI root cause and correction | Old harness relied on a race between remote EOF observation and the next request submission to make endpoint 1 receive business. Endpoint 0 now deterministically keeps each odd request connection and drops every even recorded attempt before any response byte; retry must start at endpoint 1. Tests require both servers, nonzero retries, every retry cross-endpoint, and zero same-endpoint retries |
| 2026-07-15 | Corrected generation-stress evidence | Ten consecutive 1,000-generation nodes passed. A retained diagnostic produced generation `1000`, accepts `[500, 500]`, business attempts `[1000, 500]`, attempts/retries/cross-endpoint `1500/500/500`, same-endpoint `0`, 1,000 unique responses, and zero cross-request/cross-generation completions. Full stress file **5 passed in 64.73s**; complete suite **443 passed in 80.81s** |
| 2026-07-15 | Generation-stress independent review | **CLEAN**: two real loopback endpoints are deterministically exercised, retry provenance binds the final server/connection/global attempt, Actor identity is stable, and close resource assertions remain non-vacuous; compile/diff/process checks passed with no pytest left behind |
| 2026-07-15 | Exact `a53cc09` remote checks | CI run `29403736004` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29403735977` passed. PR #12 remained OPEN and draft at the exact SHA |
| 2026-07-15 | FIFO-v2 producer baseline/current loopback probes | All nine pool 1/2/4 x concurrency 1/10/100 five-request cases on clean `71089c0` and current source had exact request/attempt/record counts, connection coverage equal to pool size, unique responses, and zero cross-request/cross-generation completions; temporary files were removed |
| 2026-07-15 | FIFO-v2 initial evidence matrix | Schema 4 and root/provenance regressions **53 passed in 0.85s**; full suite **455 passed in 80.96s**; compileall, wheel/sdist, MkDocs strict, and diff check passed |
| 2026-07-15 | FIFO-v2 adversarial protocol red evidence | After the first root/provenance blockers were closed, probes still accepted an unbound trial label, a 1 microsecond trial containing multi-millisecond cases, whole-cell replay, and then single-case/cross-role exact replay. Deterministic regressions were added before each verifier correction |
| 2026-07-15 | FIFO-v2 final local evidence | Trial label, physical duration, per-case cross-role exact replay, and boolean/zero-or-exact cohort boundary constraints pass; runner cwd/source-root, physical dirty-root, sequential attempt, epoch, connection coverage, and cohort wave mutation tests are retained. Evidence file **57 passed in 0.86s**; complete suite **459 passed in 82.77s** |
| 2026-07-15 | FIFO-v2 evidence test audit | **CLEAN**: production root routing mutations make the runner test fail; dirty physical root and sequential self-consistent attempt swaps are target-locked; fixture uniqueness preserves exact throughput and latency boundaries |
| 2026-07-15 | FIFO-v2 protocol audit | **CLEAN** at the frozen plan trust boundary after red-to-green root/SHA, epoch, connection, attempt, wave, label, duration, replay, and boundary type/count probes. Deliberate synchronized fabrication of trusted raw measurements is explicitly not claimed as cryptographically authenticated |
| 2026-07-15 | Exact `66e4496` remote checks | Pages run `29407448038` passed. CI run `29407448048` passed Ubuntu 3.10-3.13, Windows 3.13, and package build, but Windows 3.11 failed only `test_broker_delayed_assignment_return_reclaims_expired_lease`: fixed `sleep(0.06)` returned before the test's 50 ms deadline on that runner, so the retained result was a valid lease rather than the expected timeout |
| 2026-07-15 | Windows 3.11 regression timing correction | `DelayedReturnEvent` records the exact deadline derived from its real `wait(timeout)` call; the test blocks to that monotonic deadline before allowing the wait to return. The node passed **20 consecutive rounds**, the pool regression file **46 passed in 1.61s**, and the full suite **459 passed in 82.19s** |
| 2026-07-15 | Exact `72ef660` remote checks | CI run `29407921165` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29407921155` passed. PR #12 remained OPEN and draft at the exact SHA |
| 2026-07-15 | FIFO-v2 campaign A declaration | Campaign `fifo-v2-72ef660-a`; canonical declaration SHA256 `fafe65ea46f182cdc06d887043758a2c6e6a72c4c7a68de30853a111a3877f39`; declaration file SHA256 `bb64e08537b9f47b518d196f86635ea7bf5ee6e2cd6d6a48264564571834ca00`; clean roots `71089c0` and `72ef660`; exact `ABBA + BAAB` schedule externally recorded before sampling |
| 2026-07-15 | FIFO-v2 campaign A run | All eight attempt-1 trials completed once in 2,050.1 seconds. Evidence structure was clean across 1,000,000 rows, but five hard gates failed: sequential/saturated throughput ratios 0.937723/0.889017, sequential p99 delta 0.8245 ms, and no-backlog p50/p99 deltas 0.9764/1.7462 ms. Bundle SHA256 `a533a286a8c3f988b32dd1303ef174a8cbd18067095a92909a0587fc6c79c29d`; report SHA256 `5dc53c3c336c417a7a6d610afd4df6929f8461ab43475f01e0e082778376df0b`; overall **FAIL**, no exact-source rerun |
| 2026-07-15 | FIFO-v2 campaign A independent artifact audit | Evidence **CLEAN**, performance **stable FAIL**: roots and hashes exact; all completion identities and digests valid; every adjacent pair and both counterbalanced blocks retain the throughput/no-backlog regression, so no single cell explains the result |
| 2026-07-15 | Post-failure send-path diagnosis | Sequential call-to-server-read was 182.5/371.5 microseconds baseline/current and four-worker no-backlog was 258.8/928.4 microseconds; response construction-to-route was effectively unchanged, isolating the dominant loss before wire send rather than parser, response routing, or heartbeat |
| 2026-07-15 | Wake-only Actor drive | Selector batches record wake/TCP presence and advance in the same iteration only for wake-only batches with no decoded frames or decoder buffer. Mixed TCP batches remain READ-first and STOP/cancel is drained first. Focused Actor/Pool/Lifecycle/Failover checks passed, and a deterministic five-case batch predicate test covers TCP, decoded, and partial-buffer suppression |
| 2026-07-15 | Broker allocation and scan optimization | Immediate idle leases retain `cancellation=None`; queued leases retain the exact waiter cancellation Event; the unused `published` Event was removed in favor of Condition-protected state; reclaim is performed exactly once per assign. A shared set cancellation sentinel preserves lazy capacity recovery after release timeout, constructor failure, and abandon |
| 2026-07-15 | Broker isolated C/E/E/C microbenchmark | Clean `72ef660` acquire/validate/release measured 13.754/11.956 microseconds per operation. Removing the idle Event measured 6.789/7.351 microseconds; the final one-reclaim implementation measured 5.416/5.274 microseconds. Four new regressions cover event identity, release-timeout recovery, abandon, and append-to-assign concurrent cancellation for atomic batches |
| 2026-07-15 | Low-risk loopback development C/E/E/C | Fixed 1,500 sequential, 10,000 saturated, and 500 four-worker waves ran in control/experiment/experiment/control order with all cross counters zero. Adjacent saturated throughput improved 627.975 to 639.716 and 579.449 to 586.635 rps; sequential and no-backlog showed system drift, so these samples are diagnostic only and cannot replace a formal campaign |
| 2026-07-15 | Independent hot-path reviews | Actor wake-only review found no receive-boundary or priority violation after the batch regression was added. Broker review found and reproduced two capacity/progress gaps in early drafts; `_CANCELLED_LEASE`, abandon marking, and release-time concurrent-cancellation reclaim closed them. Final focused review was **CLEAN**, with **62 Pool tests passed** independently |
| 2026-07-15 | Rejected runtime fast path | Lock microbenchmarks put Event, Lock/Condition, registration, Broker validate, and `_ensure_started_before` costs in the sub-microsecond to low-tens-of-microseconds range. Removing strong epoch/broker/fatal checks could not provide the required gain and would weaken late-registration and close contracts; no runtime fast-path source change remains |
| 2026-07-15 | Rejected terminal handoff experiment | A deterministic red test proved the old successor was submitted by the waiting worker, then turned green when the Actor terminal callback submitted it outside the Broker lock. The implementation passed 63 Pool and 108 Actor/Failover tests, but added only about 0.8 percent saturated throughput versus adjacent clean `72ef660` and did not improve no-backlog. The entire handoff, socket split, and test migration were precisely reverted |
| 2026-07-15 | Post-revert focused verification | Actor/Failover/Pool/Lifecycle regression files **202 passed in 14.84s**; compileall and diff check passed. `socket.py` has no source diff from `72ef660`; only the low-risk Actor/Broker changes and their regressions remain |
| 2026-07-15 | Low-risk optimization checkpoint local matrix | Complete suite **468 passed in 82.20s**; wheel and sdist built successfully; MkDocs strict and `compileall -q src tests scripts` passed |
| 2026-07-15 | Low-risk optimization source checkpoint | Commit `8296511` (`Fix-Checkpoint: F06-HOTPATH-LOW-RISK`) contains only Actor/Broker production changes and four regression groups; the user-owned result document was explicitly excluded |
| 2026-07-15 | Exact `d76ca47` remote checks | CI run `29416793618` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29416793666` passed. PR #12 remained OPEN and draft at the exact SHA |
| 2026-07-15 | Exact `2da7651` remote checks | CI run `29417093204` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29417093233` passed. PR #12 remained OPEN and draft at exact head `2da76518a785c6c167474b9826863c1d3cf98953` |
| 2026-07-15 | Formal `fifo-v2-2da7651-a` campaign | Eight declared attempt-1 cells completed once in exact ABBA+BAAB order; the external campaign command wall time was 2,050.8 seconds. The verifier found no evidence errors across 1,000,000 completions and all cross counters were zero, but sequential/saturated throughput ratios 0.948088/0.939764 and no-backlog p50/p99 deltas 0.73185/1.2889 ms failed. Bundle `2d10ed3...95ee3c` and report `cc8cd944...dc0e1` are retained permanently as FAIL |
| 2026-07-15 | Rejected Broker no-waiter fast path | Skipping the empty-waiter assignment scan and retaining a monotonic reclaim marker reduced an isolated Broker operation by roughly 10-18 percent but less than one microsecond absolute. Real pool-4/cohort-4 p99 regressed in both adjacent pairs by about 0.177/0.089 ms. The experiment was fully reverted; no source or test diff remains |
| 2026-07-15 | Exact `d9619b0` campaign-evidence checks | CI run `29421934749` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29421934716` passed. PR #12 remained OPEN and draft at exact head `d9619b0e56d38c4d0105ac9491fd96892d2ff79e` |
| 2026-07-15 | Rejected conditional lock-ownership allocation | Four Actor/Failover/Pool/Lifecycle regression files passed **202 tests in 14.36s**. Pure allocation saved about 157 ns/call, but the fixed C/E/E/C loopback sequence was not stable: sequential rps were 165.225/163.523/163.101/164.264; saturated rps 639.902/642.661/628.212/642.711; no-backlog p50 6.7791/6.7390/6.8634/6.7603 ms; and p99 7.9479/8.0148/8.1170/7.8515 ms. Every one of 54,000 measured completions was unique with zero cross counters. Both experiment sequential cells and both no-backlog p99 cells regressed against adjacent controls, so production and test edits were fully reverted |
| 2026-07-15 | Exact `052ff68` rejected-experiment checks | CI run `29422693189` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29422693103` passed. PR #12 remained OPEN and draft at exact head `052ff687fd4db5899e95296a307f5610b5a44e3e` |
| 2026-07-15 | Rejected queued-success return fast path | A deterministic baseline run produced the intended **1 failed, 1 passed**: empty-success return rescanned, while the successor/cancelled-capacity counterexample progressed. The implementation then passed four focused nodes, **52 Pool tests**, and **204 Actor/Failover/Pool/Lifecycle tests in 14.20s**. Fresh C/E/E/C loopback values were sequential rps 164.541/163.927/164.715/165.550; saturated rps 641.091/633.767/634.805/632.902; no-backlog p50 6.7534/6.8481/6.7919/6.7335 ms; and p99 7.9461/8.4115/8.0537/7.7909 ms. All 54,000 completions were unique with zero cross counters. Saturated improved only in the second adjacent block and regressed in the first; both no-backlog p99 cells regressed. Production and experimental tests were fully reverted |
| 2026-07-15 | Exact `76c3a95` rejected-experiment checks | CI run `29423544748` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29423544439` passed. PR #12 remained OPEN and draft at exact head `76c3a953f45dc7e9547b247c810f781c2169a557` |
| 2026-07-15 | Cross-thread timing localization | Ticket-identity instrumentation covered all 2,000 four-worker no-backlog requests. Publish-to-Actor p50/p99 was 0.5305/1.3778 ms and publish-to-send 0.6104/1.5163 ms, while Actor-active-to-send was only 0.0639/0.4165 ms and Broker-plus-facade medians totaled about 0.069 ms. A second trace split wake entry: publish-to-wake was 0.0048/0.0138 ms, caller-side elapsed time inside the Python wake send call was 0.4249/1.4234 ms under GIL scheduling, and wake-return-to-Actor was 0.1083/0.8671 ms. This rejected further sub-microsecond Broker edits and localized the material Windows scheduling handoff |
| 2026-07-15 | Rejected scheduling diagnostics | Caller-side `Sleep(0)`, unconditional/double Actor terminal yields, above-normal Actor thread priority, GIL-held `ws2_32.send`, and slot-aware yield probes either lacked stable no-backlog benefit or regressed sequential/saturated throughput. No source, ctypes, priority, or temporary instrumentation remains |
| 2026-07-15 | Retained Windows Actor cooperation policy | Windows pool-size one uses one terminal `Sleep(0)` only after an external ticket is terminal and no pending/cancel/stop is visible. Windows multi-slot pools use a 0.5 ms interruptible successor Event grace; Linux and standalone remain unchanged. The multi-slot C/E/E/C sequence used control HEAD `2da7651`, 10,000 saturated requests and 500 four-worker cohorts per cell: saturated rps were 665.234/670.493/672.009/664.404, no-backlog p50 6.6034/6.5594/6.5111/6.6500 ms, and p99 7.7608/7.7578/7.5593/7.7238 ms. Adjacent changes were saturated +0.791/+1.145 percent, p50 -0.044/-0.139 ms, and p99 -0.003/-0.165 ms. After the control-priority correction, a separate final 3,000-request sequential C/E/E/C measured 167.844/167.744/168.344/167.729 rps, p50 5.9059/5.9151/5.8804/5.9401 ms, and p99 6.6611/6.6830/6.6395/6.6165 ms: adjacent throughput changes -0.060/+0.367 percent. All 12,000 final sequential completions and all 48,000 retained multi-slot completions were unique with zero cross counters. These are development evidence, not a formal gate |
| 2026-07-15 | Cooperation adversarial reviews and corrections | Independent reviews found terminal yield bypassing already-visible control priority and an `abandon_actor()` stop signal racing between Event check and clear. Unified control-lock checks, a post-clear finalizer stop recheck, and deterministic grace/yield x internal/pending/cancel/stop plus finalizer lost-wake tests closed both. Final race and lifecycle/configuration reviews were **CLEAN**, including real pool configuration reset/reopen and Socket-to-Actor propagation coverage |
| 2026-07-15 | Cooperation candidate local matrix | Four Actor/Failover/Pool/Lifecycle files passed **219 tests in 14.23s**. Final complete suite passed **485 tests in 77.88s** after both review corrections and configuration tests. Wheel and sdist built successfully; MkDocs strict, `compileall -q src tests scripts`, and `git diff --check` passed |
| 2026-07-15 | Windows cooperation source checkpoint | Commit `e106ad4` (`Fix-Checkpoint: F06-WINDOWS-ACTOR-COOPERATION`) contains only Actor/Socket/Pool configuration, deterministic race/configuration tests, and the progress ledger; the user-owned result document was explicitly excluded |
| 2026-07-15 | Exact `e106ad4` remote checks | CI run `29429941357` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29429941406` passed. PR #12 remained OPEN and draft at exact head `e106ad484ff90aa5602a24926ca527486197fda9` |

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
inject same-batch duplicate-response poison frames, partial-frame EOF,
reconnects, push, and concurrency; exact future-ID collision remains covered by
the fixed-key receive-boundary regressions. Artifacts explicitly report unique, duplicate, missing,
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

Commit and push this exact remote-evidence ledger update while explicitly
excluding the user-owned result document, then require that documentation-only
HEAD's CI and Pages. Create a clean detached current worktree, a new FIFO-v2
campaign ID and declaration bound to that exact SHA, and execute all eight cells
once in the frozen order. Never resample `fifo-v1-ca43972-a`,
`fifo-v2-72ef660-a`, or `fifo-v2-2da7651-a`.
