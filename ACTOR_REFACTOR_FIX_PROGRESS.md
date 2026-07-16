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
| Draft PR | [#12](https://github.com/electkismet/eltdx/pull/12), confirmed OPEN and draft at pushed HEAD `907e3e69bc8c8a38e1b8bd39af1f0bf0ecd38789` |
| Final-review correction base | `cc46e6042e60b1d70732ae813b089f9c8b572572` |
| Latest pushed correction checkpoint | `7e78bca98bbf1380145dd041fb6db6005570fc48`; exact CI run `29490966892` and Pages run `29490966925` passed |
| Current local follow-up | Successor skip-send and pre-send consolidation each failed fixed development stability rules and were fully removed; evaluate a materially different exact-epoch snapshot candidate without resampling prior source |
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
| F01 receive ordering and boundaries | CORRECTNESS CLOSED; CHECKPOINT CANDIDATE | Every send step drains Actor-observed receive state; partial decoder interest is READ-only; >64 tail continuation resumes safely; recv batch size mathematically caps decoded queue at 1024 |
| F02 request identity and build isolation | CORRECTNESS CLOSED; CHECKPOINT CANDIDATE | Exact cancel, terminal claim, FIFO request/control/submission identity gates, physical-lock interruption recovery, and build isolation pass |
| F03 connect and failover | COMPLETE (`2e48be0`) | Candidate/attempt budgets, next-endpoint retry, Windows peer verification, non-busy rearm, and seven real/fault-injected regressions |
| F04 Broker and pinned leases | CORRECTNESS CLOSED; CHECKPOINT CANDIDATE | Failed lease and assigned pin reservation use exact cancellation for lazy reclaim; all assignment paths reclaim before capacity checks; snapshots release waiter/Event ownership |
| F05 lifecycle and shutdown | CORRECTNESS CLOSED; CHECKPOINT CANDIDATE | Runtime registration rechecks retire after append, so either abandon snapshots the runtime or the registering thread stops it itself |
| F06 stress, performance, resources, compatibility | HEARTBEAT CHECKPOINT CLOSED; PERFORMANCE REOPENED | Exact `907e3e6` Windows 3.13 measured 0.989513 and remains FAIL. Revision-7 heartbeat and exact `7923287` CI/Pages passed, but formal `fifo-v2-7923287-a` failed sequential/saturated throughput and sequential/no-backlog p99. Exact-source performance plus heavy/resource evidence remain required |
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

### FIFO-v2 `0183c49` Campaign Failure

Campaign `fifo-v2-0183c49-a` was declared before sampling against clean
baseline `71089c0a2867a75dc79aa2c340213f4e3845b6e3` and current
`0183c496a92cf91d4bdc85405f92bc27f43cf768` worktrees. Its canonical
declaration SHA256 is
`3e3de47d6b2fabd2899efeb5cd25d3a736a6756787f9f8e329ea70aceb84ca9a`;
the declaration file SHA256 is
`7e57b5af5ae2834fbd3bc6dade4bdbb0205a550fef8b7d268b0e1fd2b19d5ef0`.
All eight attempt-1 trials ran exactly once in fixed
`baseline/current/current/baseline/current/baseline/baseline/current` order.
The external campaign command completed in 1,936 seconds with no retry. The
verifier reported `errors=[]`.

Across 32 cases and 1,000,000 completion rows, successes, server requests,
wire attempts, unique responses, and latency records were all exactly
1,000,000. Duplicate, missing, unexpected, cross-request, cross-generation,
digest mismatch, and provenance mismatch counts were all zero. The immutable
campaign bundle SHA256 is
`242381e76dcaf9d4844dfdb53af6a766f108ef735cdcc3b5bb872c80608c353d`;
the verification report file SHA256 is
`4de1e2ced997397da86444ad99b42dd57a778b3e0fd4ecab5db47c31f40076ca`.

Two hard gates failed:

- Sequential throughput was 177.610197 versus 168.187461 requests/second,
  ratio 0.946947 against the 0.95 minimum. Current needs about another
  0.322 percent.
- No-backlog p99 was 6.7586 versus 7.7079 ms, delta 0.9493 ms against a
  0.67586 ms allowance; the passing ceiling was exceeded by 0.27344 ms.

Saturated throughput passed at ratio 0.958216. Sequential p50/p99 and
no-backlog p50 also passed; no-backlog p50 was 5.9840 versus 6.5505 ms, leaving
0.0319 ms of allowance. Both failures reproduced in both blocks. ABBA
sequential/saturated ratios were 0.946994/0.957524, no-backlog p50 passed with
only 0.00248 ms of allowance, and p99 exceeded its ceiling by 0.30845 ms. BAAB
ratios were 0.946900/0.958909, no-backlog p50 passed with 0.06097 ms of
allowance, and p99 exceeded by 0.22599 ms. Independent artifact audit and
verifier replay were **CLEAN**. This campaign is permanently retained as FAIL
and this exact source will not be sampled again.

### FIFO-v2 `7923287` Campaign Failure

Campaign `fifo-v2-7923287-a` was declared before sampling against clean
baseline `71089c0a2867a75dc79aa2c340213f4e3845b6e3` and current
`79232870c337a94e5d79eca723d8bf5d09371e89` worktrees. Its canonical
declaration SHA256 is
`5ab6e75cd12d71e396c09ee592a174b7c4900be69ce606b02527e609428a6cde`;
the declaration file SHA256 is
`36ff99846b8a300f738b85abc20680686a462e07f4b79161779cf44ff9dbd484`.
All eight attempt-1 cells completed once in the fixed
`baseline/current/current/baseline/current/baseline/baseline/current` order in
2,003.2 seconds. Verifier replay returned `errors=[]`.

Across 32 distinct cases and 1,000,000 completion rows, requests, successes,
server requests, wire attempts, unique responses, completion records, and raw
latency records were all exactly 1,000,000. Duplicate, missing, unexpected,
cross-request, cross-generation, digest mismatch, provenance mismatch, and
exact case replay counts were all zero. Current fixed-cohort cases completed
10,640 exact clean boundary checks. The immutable bundle SHA256 is
`2497cf1e3efe07e449511935661da249238b4443d2a9bae906bebe0ed8373961`;
the stored report and independent replay file SHA256 values are
`954192977dee7699dfd1c8991e0dcf2694fa8a3047ab0174fb93849083ead4d1`
and `15339fd279e6330672553a7aa53d18498de6666213ddbd0dfa2549505d328b7f`.

Four hard gates failed:

- Sequential throughput was 174.410898 versus 163.773610 requests/second,
  ratio 0.939010 against the 0.95 minimum.
- Saturated throughput was 687.407879 versus 634.593945 requests/second,
  ratio 0.923169.
- Sequential p99 was 6.1812 versus 6.8017 ms, delta 0.6205 ms against a
  0.61812 ms allowance; it missed by 0.00238 ms.
- No-backlog p99 was 6.8170 versus 8.4244 ms, delta 1.6074 ms against a
  0.6817 ms allowance; it missed by 0.9257 ms.

Sequential and no-backlog p50 passed at deltas 0.3823/0.5959 ms. Saturated raw
p50/p99 remained report-only at baseline 132.56405/536.1256 ms versus current
155.13345/170.9080 ms; contended-wave report-only p50/p99 was
78.0597/150.6717 versus 89.90885/168.4938 ms. Adjacent block throughput ratios
for sequential/saturated were `0.960603/0.951229`, `0.964711/0.955723`,
`0.917135/0.897796`, and `0.914861/0.889208`; all blocks favored baseline.
Independent artifact audit classified the evidence **CLEAN** and performance
**FAIL**. This exact source and campaign are permanently retained and will not
be sampled again.

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
| 2026-07-16 | Exact `0183c49` remote checks | CI run `29430226672` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29430226404` passed. PR #12 remained OPEN and draft at exact head `0183c496a92cf91d4bdc85405f92bc27f43cf768` |
| 2026-07-16 | Formal `fifo-v2-0183c49-a` campaign | Eight attempt-1 cells completed once in exact ABBA+BAAB order in 1,936 seconds. Artifact audit and verifier replay were CLEAN across 1,000,000 completions, but sequential throughput ratio 0.946947 and no-backlog p99 excess 0.27344 ms failed in both blocks. Saturated throughput, sequential p50/p99, and no-backlog p50 passed. Bundle `242381e7...c353d` and report `4de1e2ce...076ca` are retained permanently as FAIL |
| 2026-07-16 | Post-campaign formal-size timing | With 10,000 no-backlog requests, the 0.5 ms grace hit 97.1 percent. Publish-to-send p50/p99 was 0.1212/1.0232 ms, send-to-terminal 6.1201/7.2445 ms, caller resume 0.1760/0.5349 ms, and total 6.5894/7.7507 ms. Response-side scheduling is now the dominant tail; simply extending grace cannot remove it |
| 2026-07-16 | Rejected Windows scheduling and completion probes | Response-only above-normal priority, hard affinity, GIL switch interval 1 ms, a native-lock completion signal, early caller signal, and combined ideal-processor/priority all failed to materially improve p99 or regressed throughput. Soft ideal processor improved no-backlog p99 by about 0.10-0.12 ms but was inconsistent for sequential and insufficient for the formal gate. No ctypes, affinity, priority, global interpreter setting, or alternate completion primitive remains |
| 2026-07-16 | 2 ms grace and strong-check consolidation candidate | A formal-size no-backlog C/E/E/C for 0.5 versus 2 ms measured p99 7.7777/7.6907/7.6616/7.7854 ms and p50 6.5504/6.5304/6.5083/6.5537 ms, with throughput and all completion identities clean. Per-request profiling measured redundant normal-pool `broker.validate` at 7.74 us mean and `_require_current_runtime` at 8.79 us mean. The candidate removes only the duplicate post-acquire validate and folds two lifecycle snapshots while retaining submission-gate, retire/fatal/epoch/broker guard checks. Independent race and lifecycle reviews were **CLEAN**; acquire-then-close and no-redundant-validate regressions pass |
| 2026-07-16 | Post-campaign candidate local matrix | Four Actor/Failover/Pool/Lifecycle files passed **220 tests in 14.28s**. Complete suite passed **486 tests in 79.45s**. Wheel and sdist built successfully; MkDocs strict, `compileall -q src tests scripts`, and `git diff --check` passed |
| 2026-07-16 | 2 ms grace and pool-guard checkpoint | Commit `9338286` (`Fix-Checkpoint: F06-POOL-GUARD-HOTPATH`) contains the reviewed production changes, two deterministic pool regressions, and this ledger; the user-owned result document remains excluded |
| 2026-07-16 | Exact `9338286` remote checks | CI run `29437501858` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29437501886` passed. PR #12 remained OPEN and draft at exact head `9338286f61def652cd1057986e8047b2a7ce6657` |
| 2026-07-16 | Recovery focused verification at `9338286` | Actor/Pool core files passed **178 tests in 5.40s**; the exact Actor/Failover/Pool/Lifecycle regression matrix passed **220 tests in 14.30s** |
| 2026-07-16 | Exact `2d4cea8` remote checks | CI run `29464918209` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and package build; Pages run `29464918137` passed. Ubuntu reported 485 passed plus the declared Windows-only skip; both Windows jobs reported 486 passed |
| 2026-07-16 | Post-`9338286` independent adversarial review | A/B/C review found a WRITE-only after-select receive race and a legal >1024-frame burst failure. D/E review found add-runtime/abandon Actor escape, FAILED-pin lease loss under Broker contention, and assigned pin reservation leakage. Performance campaign creation was stopped before declaration or sampling |
| 2026-07-16 | Deterministic reopened red matrix | Five exact nodes produced **5 failed in 1.81s** before any production edit: WRITE-only collision had no pre-send push; the legal burst raised `ProtocolError: decoded response frame queue exceeds limit: 1024`; guard registration returned `True` after abandon; FAILED pin lease stayed ACTIVE with idle/active `0/1`; assigned pin waiter left `pin_waiter_count=1` |
| 2026-07-16 | Follow-up deterministic red nodes | Partial-tail continuation produced **1 failed in 0.68s** with `send_calls=0` after a 65-frame tail; the strengthened max-pending admission node produced **1 failed in 0.95s** because the next normal waiter could not enter the queue |
| 2026-07-16 | Post-review correction matrix | Six exact correction nodes passed **6 tests in 0.29s**; Actor/Failover/Pool/Lifecycle regressions passed **226 tests in 14.48s**; the final exact-worktree complete suite passed **492 tests in 82.37s** |
| 2026-07-16 | Post-review correction builds | `python -m build` produced wheel and sdist; `python -m mkdocs build --strict`, `python -m compileall -q src tests scripts`, and `git diff --check` passed |
| 2026-07-16 | Post-review correction independent reviews | Actor receive/send/fairness review, Guard/pin/lifecycle review, and deterministic evidence review all returned **CLEAN** after their findings were fixed. Additional probes confirmed a send-return-time legal response succeeds, late cleanup cannot release a replacement lease, and cancelled pin reservations do not block FIFO admission |
| 2026-07-16 | Post-review correction source checkpoint | Commit `3201d3d` (`Fix-Checkpoint: F01-F05-POST-9338286`) contains the reviewed Actor/Guard/pin fixes, deterministic regressions, and ledger; the user-owned result document remains excluded |
| 2026-07-16 | Exact `3201d3d` remote checks | CI run `29466728955` passed Ubuntu 3.10-3.13, Windows 3.11/3.13, and Python 3.13 package build; Pages run `29466728894` passed strict build and artifact upload. PR #12 remained OPEN and draft at exact head `3201d3d9636c306a657881920c281c1adae40364` |
| 2026-07-16 | Exact `907e3e6` remote checks | Pages and Ubuntu 3.10-3.13 plus Windows 3.11 passed. Windows 3.13 alone failed the strict heartbeat node at aggregate elapsed ratio **0.989513**, 0.000487 below the unchanged `>0.99` gate; 491 other tests passed. `907e3e6` differs from green `3201d3d` only in this ledger, so the failure is retained as measurement evidence rather than attributed to source drift |
| 2026-07-16 | Prospective heartbeat stabilization declaration | Increase only the heartbeat test's requests per phase from 1,000 to 4,000 and assert that exact sample size. Keep four blocks, 32 phases, alternating reversed balanced order, aggregate elapsed estimator, and strict `>0.99` gate unchanged; require timed heartbeat requests to be zero in both baseline and enabled conditions. Do not change production source or reinterpret/rerun `907e3e6`; the longer phases reduce fixed scheduler-noise share and strengthen continuous-pressure exposure |
| 2026-07-16 | First isolated 4,000-request heartbeat node | **1 passed in 211.03s** after only the prospective sample-size change. Pytest did not print the passing ratio/raw phases, and a later independent review found the phase-boundary proof gap below, so this run remains diagnostic and is not final heartbeat evidence |
| 2026-07-16 | Post-failure 1,000-request diagnostic | One local diagnostic retained every raw sample: ratio `1.012137`; block ratios `1.000793/1.003732/1.015972/1.028265`; both conditions sent zero timed heartbeat; all 35,232 responses were unique with both cross counters zero. The 2.26-point opposite-direction span on identical source confirms that 1,000-request phases are not a stable decision rule at the 1% boundary |
| 2026-07-16 | Heartbeat evidence adversarial review | Reviews found no production state leak or enabled-only pre-send path, but required disabled as well as enabled timed heartbeat counts to equal zero and rejected fixed 30ms waits as proof that old internal heartbeat work was terminal. Stabilization revision 2 holds the Actor control lock while changing interval, replaces each fixed wait with four disabled business requests covering all slots plus an exact active/pending/cancel quiescence snapshot, retains all raw failure diagnostics, and keeps 4,000 requests, four blocks, 32 phases, aggregate elapsed ratio, and strict `>0.99` unchanged |
| 2026-07-16 | Heartbeat stabilization revision-2 raw run | Exact command used `--heartbeat-requests 4000 --idle-seconds 0.2`; artifact SHA256 `F20292FF66EE91C8FF034925193F5F67EA0C3D7DCF9634C58BB14190D94D17C1`, implementation `907e3e6`, dirty worktree explicitly recorded. Aggregate ratio **0.999151**; block ratios `1.016917/0.986091/0.998961/0.994645`; 131,488 response identities clean. This is diagnostic only: later review proved a heartbeat scheduled across disable could fall after a phase delta and before the next phase baseline, so its reported timed heartbeat `0/0` was not a complete attribution proof |
| 2026-07-16 | Heartbeat revision-2 phase-tail red review | Two independent reviews and a coordinated Event probe proved `_schedule_heartbeat` can retain an old interval across disable and create an internal ticket after the phase delta is read. The last phase had no later flush. This does not directly alter elapsed time after the last business future, but invalidates the strict zero-heartbeat evidence; revision 2 is not accepted as final evidence |
| 2026-07-16 | Prospective heartbeat stabilization revision 3 | Before a new sample, replace public business flush with `pool.connect()` holding all N leases and submitting an exact ConnectTicket to every slot. Run one disabled all-slot barrier before the schedule, after every warmup, and immediately after every timed workload; only after the end barrier and active/pending/cancel quiescence read the phase heartbeat delta. Expected exact slot barriers are `4 * (1 + 32 * 2) = 260`; measured response identities exclude barrier tickets. Keep 4,000 requests, four blocks, 32 phases, aggregate elapsed ratio, fixed order, and strict `>0.99` unchanged |
| 2026-07-16 | Revision-3 polluted diagnostic | A 4,000-request run overlapped an independently started barrier pytest and is rejected as performance evidence. Diagnostic only: artifact SHA256 `7603E7C80C95416CFE50C77EFF3708003CA199921563F00CD98D9DF44087B660`; ratio `1.005199`; blocks `1.007295/0.996995/1.004334/1.012350`; heartbeat `0/0`; barriers 260; 131,232 unique responses and all error/cross counters zero. The overlapping processes ended naturally and all review Agents were stopped before the clean declaration |
| 2026-07-16 | Clean revision-3 heartbeat declaration | Run exactly one task-owned process with `--heartbeat-requests 4000 --idle-seconds 0.2`, save to a new artifact path, and start only after confirming no task pytest/stress process exists. Do not use or overwrite either revision-2 or polluted revision-3 artifact. The pre-existing unrelated `uvicorn` service is user-owned and remains untouched |
| 2026-07-16 | Clean-A revision-3 attempt terminated before artifact | A lifecycle review finding arrived while the run was in progress. The task-owned process was stopped and no output artifact existed. The finding was that initial connect failure sat outside the cleanup finally and interval reset failure could skip `pool.close()`; this attempt is not evidence and is not resumed |
| 2026-07-16 | Heartbeat harness cleanup regression | Injected initial connect failure produced **1 failed in 0.45s** with `closed=False`. Initial connect now invokes close on failure, and final reset/close uses nested `try/finally`; the cleanup node plus the exact barrier-position regression passed **2 tests in 9.18s** |
| 2026-07-16 | Clean-B revision-3 heartbeat declaration | After the cleanup correction, stop all review Agents, confirm zero task pytest/stress Python processes, use a new artifact path, and run one isolated 4,000-request measurement. The measurement protocol, estimator, schedule, and strict gate are unchanged from revision 3 |
| 2026-07-16 | Clean-B revision-3 heartbeat raw result | Artifact SHA256 `BB87C0A16924F60876D7B7CDE144E39A029675E6AFF04BA6985D6BD95ACF1743`; implementation `907e3e6`, dirty worktree explicitly recorded. Aggregate ratio **1.004205** passed strict `>0.99`; blocks `1.021189/0.998317/0.997274/1.000139`; baseline/enabled timed heartbeat `0/0`; exact barriers 260; 131,232/131,232 responses unique; duplicate/missing/unexpected/cross-request/cross-generation all zero; idle CPU ratio zero |
| 2026-07-16 | Revision-3 pre-checkpoint complete suite | Exact working tree passed **494 tests in 242.60s**. This includes the two new deterministic barrier-position and initial-connect cleanup regressions |
| 2026-07-16 | Revision-3 pre-checkpoint builds | `python -m build` produced `eltdx-1.0.2.tar.gz` and `eltdx-1.0.2-py3-none-any.whl`; `python -m mkdocs build --strict`, `python -m compileall -q src tests scripts`, and `git diff --check` all passed. Diff check emitted only existing LF-to-CRLF worktree warnings |
| 2026-07-16 | Revision-3 independent adversarial reopening | One review found interval publication could expose an immediately due heartbeat before rebasing activity, the barrier checked only task mailboxes rather than wire/decoder state, and a timed business failure could be replaced by its cleanup barrier failure. A separate deadline/compatibility review found concurrent pool callers waiting on first hostname DNS retained the DNS-before deadline, plus missing pinned `request`/push success-path coverage. The second heartbeat review was otherwise clean but does not override these deterministic findings |
| 2026-07-16 | Post-review deterministic red command | `python -m pytest -q` on the new heartbeat publication, six wire/receive quiescence parameters, timed double-failure, concurrent pool DNS deadline, and pinned proxy compatibility nodes produced **9 failed, 1 passed in 1.20s**. The proxy compatibility node passed current source; the other failures exactly reproduced the reviewed gaps before production changes |
| 2026-07-16 | First post-review implementation checks | The ten-node red set passed **10 tests in 0.52s**; pool/lifecycle files passed **171 tests in 9.17s**; heartbeat helper paths passed **12 tests in 9.72s**; expanded Actor/Socket/Pool/Lifecycle/Failover/Resources/evidence correctness passed **369 tests in 15.69s** |
| 2026-07-16 | Heartbeat fix adversarial reopening | Post-patch review found the quiescence snapshot and target rebase were not one stable boundary, accepted a missing TCP generation, reused a timestamp obtained before later runtime locks, and let phase reset/outer cleanup failures replace the business/barrier cause chain. New coordinated publication, disabled-ack, missing-generation, and triple-failure regressions produced **4 failed in 0.66s** before the second implementation |
| 2026-07-16 | DNS fix adversarial reopening | Post-patch review found a DNS waiter stayed unbounded through post-resolution Actor startup and that epoch-only matching could reuse a failed attempt's deadline after same-epoch retry. Coordinated post-DNS stall and failed-attempt identity tests produced **2 failed, 1 passed in 1.04s** before the second implementation; non-empty parsed proxy drain coverage was added |
| 2026-07-16 | Late DNS publication-window reopening | A caller can begin during DNS yet first acquire the pool condition only after RUNNING publication. The exact coordinated regression returned stale pre-DNS deadline `13.0` instead of shared post-DNS deadline `43.0` and failed **1 test in 0.51s** before binding call-entry time to the exact published StartupAttempt's DNS completion |
| 2026-07-16 | Heartbeat fence evidence correction | A final concurrency review rejected treating a lock-based diagnostic snapshot as an atomic Actor boundary. The harness now relies on the stronger ordering provided by disabled all-slot ConnectTickets: each owning Actor processes its ticket after any heartbeat that raced with disable. Target interval publication then holds every runtime lock at once. Every timed sample additionally requires unchanged Actor generation IDs and server accept count across its terminal disabled fence; control/wire fields remain diagnostics rather than the claimed synchronization proof |
| 2026-07-16 | Final deadline/counting red extensions | Exact-attempt review tightened the failed same-epoch waiter expectation and reproduced an incorrect new-attempt deadline `103.0` instead of its original `13.0`. Heartbeat review required the counter baseline before target publication; the new ordering node failed because no such publication boundary existed. Combined result: **2 failed in 0.68s** before adding the DNS start lower bound and baseline-before-publication helper |
| 2026-07-16 | Final post-review local verification | Heartbeat helper paths passed **16 tests in 9.30s**; pool/lifecycle files passed **174 tests in 8.98s**; the complete suite passed **513 tests in 238.88s** on Windows CPython 3.12.6 in one isolated process |
| 2026-07-16 | Final post-review local builds | `python -m build` produced `eltdx-1.0.2.tar.gz` and `eltdx-1.0.2-py3-none-any.whl`; `python -m mkdocs build --strict`, `python -m compileall -q src tests scripts`, and `git diff --check` passed. Diff check emitted only existing LF-to-CRLF warnings |
| 2026-07-16 | Isolated revision-4 heartbeat artifact | One task-owned process ran `--heartbeat-requests 4000 --idle-seconds 0.2` after confirming no task pytest/stress process. Artifact SHA256 `F0EA922A8BDF723A99F15067AA7556C85D331D8968E9BBEBE4C46D4402C0E001`; workload SHA256 `B6405A796ECA1CB5C5201D271F4D694A649B89441CDF0CAD82C73DE851425BB9`; dirty implementation identity `907e3e6` retained. Aggregate ratio **1.000386** passed strict `>0.99`; block ratios `0.995081/1.007262/1.009010/0.990094`; baseline/enabled timed heartbeat `0/0`; barriers 260; responses 131,232/131,232 unique; duplicate/missing/unexpected/cross-request/cross-generation, generation changes, and server accept changes all zero; idle CPU ratio zero |
| 2026-07-16 | Post-DNS startup deadline red extension | Exact-worktree review found a caller beginning after DNS completion but before pool publication inherited the owner's earlier deadline. The coordinated DNS 10-to-40, owner deadline 43, caller-at-42 case returned `43.0` instead of its own `45.0` and failed **1 test in 0.55s** before restricting attempt inheritance to calls that actually cross that attempt's DNS window |
| 2026-07-16 | Pre-owner DNS overlap and cause-cycle red extensions | Exact-worktree review found a call entering before the DNS owner but acquiring the condition only after publication retained stale deadline `13.0` instead of post-DNS `43.0`; a reused exception instance formed a `primary -> cleanup -> primary` cause cycle. The exact pair failed **2 tests in 0.62s** before distinguishing unobserved overlapping calls from exact stale attempts and adding identity-aware cause-chain cycle prevention |
| 2026-07-16 | Current-delta full-suite heartbeat failure | The isolated complete suite on the then-current delta produced **1 failed, 515 passed in 248.23s**. The strict enabled timed heartbeat total was `1`, not zero. This failure is retained and not rerun unchanged. Root cause: target interval publication occurred before worker futures were submitted and reached their start barrier, leaving a scheduler-dependent window longer than the 20ms interval. Timed publication is moved into the worker Barrier action, ordered as counter baseline, interval publication, timer start, then simultaneous worker release |
| 2026-07-16 | Isolated revision-5 heartbeat artifact | After the Barrier-action correction, one task-owned process ran `--heartbeat-requests 4000 --idle-seconds 0.2`. Artifact SHA256 `4799E258C74893E8A5D9217F3D4A9A14D9529EAA1413C622CBC12E552B961576`; workload SHA256 `8C9A0C7EBFA4E56522BDC17FEA40443A6E7C6E0D0F6605716533FC2DA1728566`; dirty implementation `907e3e6`. Aggregate ratio **1.004064**; blocks `1.001096/1.007922/1.009252/0.998048`; timed heartbeat `0/0`; barriers 260; 131,232 unique responses; all duplicate/missing/unexpected/cross/generation/accept/launch-boundary mismatch counts zero |
| 2026-07-16 | Shared-descendant exception-chain reopening | Exact-worktree review found cleanup and primary could share the same existing descendant, and appending that descendant again would create a self-loop. A deterministic shared-descendant regression was added before requiring the previous cause to be absent from the cleanup chain prior to append |
| 2026-07-16 | Final current-delta local verification | The complete suite passed **518 tests in 251.02s** on Windows CPython 3.12.6 in one isolated process. `python -m build` produced the wheel and sdist; `python -m mkdocs build --strict`, `python -m compileall -q src tests scripts`, and `git diff --check` passed with only existing LF-to-CRLF warnings |
| 2026-07-16 | Isolated revision-6 heartbeat artifact | Artifact SHA256 `1AFAD36F8412506244EBEA7C4E186A64B4743AE9677342889EC1C3AB6CAAD5EB`; workload SHA256 `2D9320B9B2765D61422F2E48999E85382C0E6506302BDDBCB5B83E07040CDEEE`; dirty implementation `907e3e6`. Structural replay passed: aggregate ratio **0.996593**; blocks `1.000123/1.001685/0.988080/0.996678`; timed heartbeat `0/0`; 260 barriers; 131,232 unique responses; all duplicate/missing/unexpected/cross/generation/accept/launch-boundary mismatch counts zero; idle CPU ratio zero. Later source corrections changed the script hash, so revision-6 is historical evidence only and is not current checkpoint evidence |
| 2026-07-16 | Exact-worktree review reopening after revision-6 | Independent reviews found three blockers: a caller crossing DNS could fail its old deadline while the owner held the pool condition publishing `RUNNING`; cause-chain merging could cycle when cleanup and the previous cause shared a deeper descendant; and a reused phase/barrier exception could execute `raise error from error`. The failed-attempt DNS test also signaled waiter readiness from the main thread rather than the real `Condition.wait()` entry |
| 2026-07-16 | Tightened review red baseline | The first draft probes produced **1 failed, 2 passed**, proving the deep-shared cycle while showing the DNS and reused-error tests were not yet exact. After coordinating the second waiter monotonic read under held `RUNNING` publication and exercising the real timed phase/barrier path, the exact five-case command produced **3 failed, 2 passed in 0.75s**: DNS returned `ResponseTimeoutError`, the deep cause graph repeated `shared`, and the reused phase error was its own cause |
| 2026-07-16 | Review correction focused verification | The exact red set passed **5 tests in 0.63s**. The original and new DNS identity/deadline nodes plus cause tests passed **9 tests in 0.38s**; lifecycle and Pool regressions passed **90 tests in 2.03s**; non-long heartbeat helpers passed **21 tests, 5 deselected in 18.83s**; and the held-publication DNS node passed **20/20** isolated rounds. `compileall` and `git diff --check` passed |
| 2026-07-16 | Review correction independent re-reviews | Two read-only reviewers returned **CLEAN** on the exact corrected worktree. The heartbeat reviewer exercised deep sharing, reused/self-cause, disjoint preservation, cleanup-containing-primary, and pre-existing cycles. The DNS reviewer verified exact observable-attempt identity, every DNS terminal event path, non-DNS bounded admission, post-DNS caller deadlines, failed/replacement/close isolation, and condition/observer lock order |
| 2026-07-16 | Revision-7 heartbeat declaration | Current corrected script SHA256 `487B3131AF237B5843FE04046D51B60F136D6E22815297E29347286F93C3EA0A`. Run exactly one isolated task-owned process with `--heartbeat-requests 4000 --idle-seconds 0.2`, write only `artifacts/actor_heartbeat_revision7_worktree.json`, and do not overwrite or reinterpret revisions 2-6. Require the same four blocks, 32 phases, aggregate `>0.99`, exact timed heartbeat `0/0`, 260 barriers, current workload hash, unique completions, and zero cross/generation/accept/launch mismatches |
| 2026-07-16 | Isolated revision-7 heartbeat raw result | The single declared process completed in 221.7 seconds. Artifact SHA256 `985A800AE0AD12463F9EE21018FA180AACF901FE6E63D58D9E5667E1F7761C9E`; workload SHA256 exactly matches current script `487B3131AF237B5843FE04046D51B60F136D6E22815297E29347286F93C3EA0A`; dirty implementation identity `907e3e6` is explicit. Aggregate ratio **0.998163** passed; blocks `0.988305/1.001516/1.007085/0.995846`; timed heartbeat `0/0`; 260 barriers; 32 phases; 131,232/131,232 unique responses; duplicate/missing/unexpected/cross-request/cross-generation, generation, accept, and launch-boundary mismatches all zero; idle probe `4/4`; paced heartbeat/business `32/32`; idle CPU ratio zero |
| 2026-07-16 | Revision-7 pre-checkpoint full verification | The exact worktree passed **521 tests in 257.97s** on Windows CPython 3.12.6 in one isolated process. `python -m build` produced `eltdx-1.0.2.tar.gz` and `eltdx-1.0.2-py3-none-any.whl`; `python -m mkdocs build --strict`, `python -m compileall -q src tests scripts`, and `git diff --check` passed. Diff check emitted only existing LF-to-CRLF worktree warnings |
| 2026-07-16 | Exact `f7355c0` remote checks | CI run `29486508429` passed Ubuntu Python 3.10-3.13, Windows Actor Python 3.11/3.13, and the Python 3.13 package build. Pages run `29486508422` passed strict documentation build and artifact upload. PR #12 remained OPEN and draft at exact head `f7355c047e590f34ccefee0e576fc34e2139e01d` |
| 2026-07-16 | Exact `7923287` evidence-head remote checks | CI run `29486968573` passed Ubuntu Python 3.10-3.13, Windows Actor Python 3.11/3.13, and the Python 3.13 package build. Pages run `29486968576` passed strict documentation build and artifact upload. PR #12 remained OPEN and draft at exact head `79232870c337a94e5d79eca723d8bf5d09371e89` |
| 2026-07-16 | FIFO-v2 `7923287` campaign declaration | Before any sample, clean detached roots were verified at baseline `71089c0a2867a75dc79aa2c340213f4e3845b6e3` and current `79232870c337a94e5d79eca723d8bf5d09371e89`; evidence tests passed **57 tests in 1.71s**. Campaign `fifo-v2-7923287-a` has canonical declaration SHA256 `5ab6e75cd12d71e396c09ee592a174b7c4900be69ce606b02527e609428a6cde` and declaration file SHA256 `36FF99846B8A300F738B85ABC20680686A462E07F4B79161779CF44FF9DBD484`. The output directory contained only that declaration; schedule is exact baseline/current/current/baseline/current/baseline/baseline/current, every cell attempt 1, and no trial existed when this external record was written |

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

## Post-`9338286` Red Commands

Initial five-node baseline, before any production edit:

```powershell
python -m pytest -q tests/test_transport_actor_regressions.py::test_write_only_snapshot_drains_collision_that_arrived_before_send tests/test_transport_actor_regressions.py::test_legal_push_burst_above_decoded_queue_limit_keeps_response_live tests/test_transport_lifecycle_regressions.py::test_guard_abandon_cannot_miss_runtime_appended_after_snapshot tests/test_transport_pool_regressions.py::test_failed_pin_terminal_lazily_reclaims_lease_when_broker_is_contended tests/test_transport_pool_regressions.py::test_assigned_pin_waiter_release_failure_is_lazily_reclaimed --tb=short
```

Result: **5 failed in 1.81s**.

Fairness continuation baseline:

```powershell
python -m pytest -q tests/test_transport_actor_regressions.py::test_partial_tail_over_fairness_budget_resumes_read_only_send --tb=short
```

Result: **1 failed in 0.68s**.

Strengthened pin-capacity baseline, before assignment reclaim was added:

```powershell
python -m pytest -q tests/test_transport_pool_regressions.py::test_assigned_pin_waiter_release_failure_is_lazily_reclaimed --tb=short
```

Result: **1 failed in 0.95s** because the next normal waiter could not enter
admission without an unrelated snapshot/heartbeat reclaim.

## Exact Next Action

Complete the independent hot-path diagnosis for the clean performance failure,
add deterministic race regressions before any production edit, and retain only
an optimization that improves fixed diagnostics without weakening FIFO,
receive boundaries, lifecycle, or pool capacity. Create a new code checkpoint
and exact-head CI before declaring any successor campaign. Never rerun exact
`907e3e6` or `7923287`, and never resample
`fifo-v1-ca43972-a`, `fifo-v2-72ef660-a`, `fifo-v2-2da7651-a`, or
`fifo-v2-0183c49-a`.

## Post-`7e78bca` Successor-Cooperation Red Baseline

The frozen `fifo-v2-7923287-a` campaign is structurally clean but failed all
four aggregate performance gates. Its exact source and campaign will not be
sampled again. Read-only diagnosis ranked the redundant Windows socketpair
wakeup at the top: the Actor explicitly cooperates for its next caller after a
successful external response, but submit/cancel/stop still write the socketpair
while the Actor is already waiting or yielding for that exact control work.

Before any production edit, deterministic grace, terminal-yield, exact pending
cancel, and STOP regressions were added alongside two green safety controls for
post-window notification and the non-blocking finalizer fallback. The exact
command was:

```powershell
python -m pytest -q tests/test_transport_actor_regressions.py::test_successor_cooperation_skips_redundant_socket_wakeup tests/test_transport_actor_regressions.py::test_successor_notifier_sends_after_cooperation_window_exits tests/test_transport_actor_regressions.py::test_successor_cooperation_keeps_pending_cancel_visible_without_socket_wakeup tests/test_transport_actor_regressions.py::test_successor_cooperation_wakes_stop_without_socket_send tests/test_transport_actor_regressions.py::test_successor_cooperation_finalizer_keeps_nonblocking_socket_fallback --tb=short
```

Result: **4 failed, 2 passed in 0.94s**. Both grace and yield modes wrote the
socket; cancel wrote twice; STOP wrote once. Notification after the cooperation
window still wrote once, and `abandon_actor()` retained its conservative
best-effort socket fallback. Production changes must make the four red cases
pass without weakening those two green controls.

The first implementation binds the decision to the existing publication
critical section: Actor entry/exit and submit/cancel/normal-stop publication
all use the exact runtime `control_lock`; grace uses the Event while terminal
yield needs no extra Event signal; every physical socket send remains outside
the lock. The non-blocking finalizer retains its unconditional Event/socket
fallback. Event-set failure falls back to a physical socket wake, and
wait/yield exceptions clear cooperation in `finally`.

Focused successor and wakeup failure checks passed **15 tests** across the new
race/exception nodes and the retained notify-failure ticket terminalization.
The expanded Actor/Pool/lifecycle six-file matrix passed **300 tests in
13.00s**. Exact remote `7e78bca` CI independently passed Ubuntu Python
3.10-3.13, Windows Actor Python 3.11/3.13, package build, and Pages before this
uncommitted implementation was measured.

A current-diff adversarial review then found that explicit private
`_notify_actor(runtime, None)` no longer reloaded the current writer as it did
at `7e78bca`; the exact compatibility regression failed **1 test in 0.71s**
before distinguishing an unprepared legacy `None` from a prepared publisher's
exact no-writer snapshot.

## Successor-Cooperation Development Diagnostic Declaration

This is an isolated development decision aid, not a formal acceptance
campaign and not a rerun of `fifo-v2-7923287-a`. Both control and experiment
use the same dirty source bytes and the current benchmark script SHA256
`B09AB7130752AE0C562B63BA04D2B1BEA42F1E168C060F13D6E86E9BBA277B84`.
Control disables only the new skip decision at runtime, so it retains the old
physical socket send without restoring or sampling exact `7923287` source.

Run exactly one C/E/E/C sequence, with no concurrent task pytest/stress/
benchmark process. Each cell uses a fresh real loopback transport/server and
contains, in this order: 1,500 sequential requests after 300 warmups; 10,000
pool-4/concurrency-100 saturated requests after 500 warmups; and 500
pool-4/four-worker fixed cohorts after 50 warmup cohorts. Server delay is 5ms.
Retain every raw latency and completion record plus source/script identity in a
single external artifact under `C:\Users\ax\Desktop\eltdx\artifacts`. Require
all 54,000 measured requests to succeed uniquely with duplicate, missing,
unexpected, cross-request, cross-generation, record/provenance mismatch, and
boundary-cleanup errors all zero. Compare both adjacent C/E blocks; reject the
candidate if sequential, saturated, or no-backlog tail does not improve
stably. These samples cannot replace a formal FIFO-v2 campaign.

The one declared process completed all four cells in exact C/E/E/C order.
Artifact `successor-cooperation-dev-ceec-7e78bca-dirty.json` is 2,491,396
bytes with SHA256
`C796C04E3C9AC1178A6A2B417CC772143483FF8E1FA2AAFDFD8A51DC6CC92AB2`.
It contains exactly 54,000 requests, successes, server requests, unique
responses, and completion records; all duplicate/missing/unexpected/
cross-request/cross-generation counts are zero, and all 2,200 cohort boundary
checks are clean.

The candidate **FAILED** the predeclared stability rule. Adjacent C0/E1 and
C3/E2 throughput ratios were sequential `0.968826/0.972417`, saturated
`0.975131/1.012629`, and no-backlog `1.028561/0.999493`. Corresponding p99
deltas were sequential `+0.0960/+0.2040ms`, saturated
`+6.5276/+5.5127ms`, and no-backlog `+0.0371/-0.1288ms`. Sequential regressed
in both blocks, saturated throughput disagreed by direction, and no-backlog
p99 disagreed by direction. The production candidate, its deterministic
experimental tests, and temporary runner are therefore removed; the raw
artifact and this rejection record remain. No formal campaign was run.

## Post-`29b250e` Pre-Send Consolidation Red Baseline

The next read-only design review allowed only two narrow changes. Admission may
skip its speculative receive when both the decoded queue and decoder buffer are
empty, but any user-space frame or partial frame must still be drained before
creating `WireExchange`. `_send_generation` must retain its first control/
STOP/expiry/identity sweep, the unconditional final receive boundary, the
post-receive generation identity and decoder/fairness gates, and the locked
send claim. Only the second post-receive control/expiry/identity sweep may be
replaced by that exact atomic claim.

Before production edits, two deterministic call-count regressions were added:

```powershell
python -m pytest -q tests/test_transport_actor_regressions.py::test_empty_userspace_receive_state_uses_only_final_presend_drain tests/test_transport_actor_regressions.py::test_final_presend_drain_uses_atomic_claim_without_second_control_sweep --tb=short
```

Result: **2 failed in 0.73s**. Current source performed two receive passes for
an empty user-space admission and two control sweeps around the final receive.
The retained collision, partial head/tail, >64 fairness, EOF, STOP/cancel,
deadline, and send-claim tests remain correctness gates for the candidate.

The implementation narrows admission's speculative receive condition to
actual decoded frames or decoder-buffer bytes, and routes the post-boundary
decision directly through the existing locked send claim. Strengthened tests
inject an exact future-ID collision only at the final control fence, prove the
single final receive classifies it before send, and require cancel, STOP, and a
crossed deadline published inside final receive to reach and be rejected by
the atomic claim. EOF and a second collision on the >64 fairness resume path
also remain no-write/boundary controls. The focused set passed **16 tests in
0.61s**; the expanded Actor/Pool/lifecycle six-file matrix passed **297 tests
in 13.61s**.

Current-diff adversarial review then found a combined STOP/deadline regression:
after the atomic claim rejected STOP, its shared failure cleanup still expired
the ticket. The deterministic final-receive STOP plus crossed-deadline node
failed **1 test in 0.84s**, observing `FAILED` instead of retaining `SENDING`
for shutdown. Failed-claim cleanup now drains control and skips expiry while
STOP is authoritative. The strengthened pre-send matrix passed **17 tests in
0.31s** after the correction. Two current-diff adversarial reviews returned
**CLEAN**, and the post-correction Actor/Pool/lifecycle six-file matrix passed
**298 tests in 13.52s**.

## Pre-Send Consolidation Development Diagnostic Declaration

This is a development decision aid, not formal acceptance and not a sample of
exact `7923287` or `29b250e` source. Both roles execute the same dirty candidate
bytes (Actor SHA256
`28E6D1417E371C078455F62C826323445AE2D003A4E474A24E681E3289CAE243`).
The control role replaces only `_advance_active_task` and `_send_generation`
in-process with their pre-consolidation behavior; experiment uses the candidate
functions. Workload SHA256 is
`B09AB7130752AE0C562B63BA04D2B1BEA42F1E168C060F13D6E86E9BBA277B84`;
one-use diagnostic SHA256 is
`A97A90B5D586A3BFF6E1078BEB8D4CBDD880CD2ACDB354505458F6041074BD95`.

Run exactly one C/E/E/C process with no overlapping task pytest/stress/
benchmark process. Every fresh-loopback cell runs 1,500 sequential requests
after 300 warmups, 10,000 pool-4/concurrency-100 requests after 500 warmups,
and 500 four-worker cohorts after 50 warmup cohorts, all at 5ms server delay.
Retain every raw latency/completion in
`presend-consolidation-dev-ceec-29b250e-dirty.json`; require exactly 54,000
unique successes/server requests/records, zero error/duplicate/missing/
unexpected/cross-request/cross-generation counts, and 2,200 clean cohort
boundaries. Compare both adjacent C/E blocks and reject unless sequential,
saturated, and no-backlog tail improve stably. The result cannot replace or
trigger early termination of a formal FIFO-v2 campaign.

The one declared process completed all four cells once in exact C/E/E/C
order. Artifact `presend-consolidation-dev-ceec-29b250e-dirty.json` is
2,491,417 bytes with SHA256
`51F10B82C89C70EF4D255E689CDE004DBB2AA483172D5634216AB9E0EE1F7AFF`.
It contains exactly 54,000 unique requests/successes/server requests/records,
zero error/duplicate/missing/unexpected/cross-request/cross-generation counts,
and all 2,200 cohort boundaries are clean.

The standalone candidate **FAILED** the predeclared stable-improvement rule.
Adjacent C0/E1 and C3/E2 throughput ratios were sequential
`1.000608/1.022882`, saturated `1.035906/0.993275`, and no-backlog
`1.107552/1.003878`. P99 deltas were sequential `-0.0321/-0.1348ms`,
saturated `-5.0225/-5.2009ms`, and no-backlog `-0.9976/-0.3957ms`.
Although every tail metric improved in both blocks, saturated throughput
reversed direction in the second block. The rule was frozen before sampling,
so the source cannot be retained or resampled standalone. Production/tests and
the temporary runner are removed; the raw artifact and rejection remain. No
formal campaign was run.

## Post-`f0a329a` Exact-Epoch Snapshot Red Baseline

The next materially different candidate targets repeated steady-state pool
guard scans. The design captures the exact epoch's broker, registration tuple,
and monotonic retire Event under the same pool condition that publishes
`RUNNING`. A pooled execute must still acquire the exact Broker lease, hold the
slot submission gate, validate runtime/registration identity under the slot
lifecycle lock, and reject the retire Event both before submission and after
Actor completion. Fatal and every shutdown path must set that same event before
cleanup/reconfiguration; old events are never cleared or reused.

Before production edits, the exact nodes were:

```powershell
python -m pytest -q tests/test_transport_lifecycle_regressions.py::test_guard_failure_sets_exact_epoch_retire_event_before_cleanup tests/test_transport_pool_regressions.py::test_pooled_execute_uses_exact_epoch_snapshot_without_guard_rescans --tb=short
```

Result: **2 failed in 1.53s**. Direct accepted `PoolRuntimeGuard.fail()` left
the epoch retire Event unset, and ordinary pooled execute entered the guard
failure rescan before lease admission. Close-after-acquire, atomic slot retire/
submission, fatal/reopen identity, and response-delivery retirement tests remain
mandatory safety gates.

The candidate publishes `PoolExecutionEpoch` with the exact broker, monotonic
retire Event, and registration tuple. Normal execute validates the snapshot
broker, lease epoch, registration epoch/broker/Event identity, and slot runtime/
callback identity under the existing submission gate; post-response delivery
checks only the old Event. Accepted guard failure now sets that Event before
Broker, PushBuffer, or Actor cleanup. Exact wrong-broker/wrong-Event and
close-plus-reopen paused-response regressions passed with lease capacity fully
returned. The focused matrix passed **8 tests in 1.22s**; Pool/lifecycle four
files passed **183 tests in 9.51s**; the six-file Actor/Pool/lifecycle matrix
passed **297 tests in 13.28s**.

## Exact-Epoch Snapshot Post-Parse/Fatal Corrections

A current-byte adversarial review paused `parse_command_response()` after the
first delivery fence, completed close plus reopen, and reproduced the old
epoch value returning successfully. `_execute_with_lease()` now checks the
same monotonic retire Event again after parsing and before cache mutation or
return. A second regression pauses parsing while direct accepted
`PoolRuntimeGuard.fail()` has set the exact Event but remains blocked inside
`broker.close()`. The caller must already receive `ConnectionClosedError`
while cleanup is still paused and the exact old Broker has zero active leases.
The test calls `PoolRuntimeGuard.fail()` directly so deleting the candidate's
Event publication makes it fail; it does not rely on `ActorFatalHandle`'s
earlier defensive Event set. Its failure cleanup only joins started threads
and unconditionally closes the pool in a nested `finally`.

Post-correction commands and results:

```powershell
python -m pytest -q tests/test_transport_lifecycle_regressions.py::test_guard_failure_sets_exact_epoch_retire_event_before_cleanup tests/test_transport_lifecycle_regressions.py::test_fatal_retire_event_rejects_inflight_delivery_before_cleanup_finishes --tb=short
# 2 passed in 0.73s

python -m pytest -q tests/test_transport_pool_regressions.py tests/test_transport_pool.py tests/test_transport_lifecycle_regressions.py tests/test_transport_lifecycle.py --tb=short
# 185 passed in 9.35s

python -m pytest -q tests/test_transport_actor_regressions.py tests/test_transport_actor.py tests/test_transport_pool_regressions.py tests/test_transport_pool.py tests/test_transport_lifecycle_regressions.py tests/test_transport_lifecycle.py --tb=short
# 299 passed in 13.01s
```

Two independent current-byte read-only reviews are **CLEAN**. One repeated the
new fatal node ten times and the exact/fatal set in a single process, observed
no Actor or scripted-server thread residue, and demonstrated that removing the
post-parse fence returns `86`. The other verified direct guard failure sets the
Event under the guard lock before Broker/push/Actor cleanup, the cleanup-paused
test fails against the missing-set behavior, and all red-path cleanup remains
bounded.

## Exact-Epoch Snapshot Development Diagnostic Declaration

This is one isolated development decision aid, not a formal acceptance
campaign and not a rerun or control sample of exact `7923287`, `29b250e`, or
any earlier source. Both roles execute the same dirty candidate production
bytes. The control role replaces only normal
`PooledSocketTransport.execute()` in-process with the `f0a329a` steady-state
behavior. After `connect()` has completed, control execute acquires the same
pool condition exactly once and directly executes the `f0a329a` already-
connected `RUNNING` fast path, including one runtime-guard failure scan and the
published-startup deadline carry, then releases it before lease acquisition.
Any non-steady state aborts the diagnostic instead of entering a different
startup path. Control captures no exact execution snapshot, and submission/
response delivery receive only the numeric runtime epoch so they perform
lifecycle plus registration/guard rescans. Experiment restores the current
exact Broker, registration, and retire-Event path. Direct guard Event
publication, the new post-parse fence, Actor/socket behavior, and every other
production function remain the current candidate in both roles.

Frozen identities are pool SHA256
`C1C57CB07D8754C0C95D0E091EA131E419805D9306D70913B284E3BC660FB67D`,
socket SHA256
`48D1A5EDEB26C59D058868B72326DEE914FAB4082698EF51F1F362F2EFE4E83D`,
workload SHA256
`B09AB7130752AE0C562B63BA04D2B1BEA42F1E168C060F13D6E86E9BBA277B84`,
and one-use diagnostic SHA256
`AB9903015820DAF6E8D283321F3CAB84F650C9A49DF9FA263CE150C05995E01B`.
The target and exclusive reservation
`C:\Users\ax\Desktop\eltdx\artifacts\exact-epoch-snapshot-dev-ceec-f0a329a-dirty.json`
and `.json.reserved` companion did not exist at declaration time. Execution
must pass the declared diagnostic SHA in
`ELTDX_EXACT_EPOCH_DIAGNOSTIC_SHA256`; pool, socket, workload, and runner hashes
are verified before reservation, before and after every cell, and after the
last cell.

Run exactly one C/E/E/C process with no overlapping task pytest, stress, or
benchmark process. Every fresh-loopback cell runs 1,500 sequential requests
after 300 warmups, 10,000 pool-4/concurrency-100 requests after 500 warmups,
and 500 pool-4/four-worker cohorts after 50 warmup cohorts, all at 5ms server
delay. Retain every raw latency and completion record. The artifact must hold
exactly 54,000 unique successes, server requests, and records; all error,
duplicate, missing, unexpected, cross-request, cross-generation, provenance,
and response-count fields must be zero/consistent; each cell must report
`cleanup_complete=true`; all 2,200 cohort boundaries must be clean. Startup
uses an exclusive-create reservation before any sample. The artifact is then
written before each active cell and atomically replaced after every completed
cell, so crash/interruption remains a permanent incomplete one-use attempt.

The retain rule is frozen before sampling: compare adjacent C0/E1 and C3/E2
blocks using E1/C0 and E2/C3. The runner computes the verdict only from raw
integers: throughput passes when
`experiment_requests * control_elapsed_ns >= control_requests * experiment_elapsed_ns`;
p99 is `sorted(latency_ns)[((n - 1) * 99) // 100]` and passes when experiment
p99 ns is no greater than control p99 ns. Sequential, saturated, and
no-backlog must pass both metrics in both adjacent blocks. Rounded summary
fields are never used. Any directional reversal, metric regression, identity/
count mismatch, or incomplete cell rejects the candidate permanently; it may
not be resampled standalone. Passing only permits the candidate to proceed to
full verification and a brand-new formal FIFO-v2 campaign.

The one declared process completed all four cells once in exact C/E/E/C order
in 131.7 seconds. Artifact
`exact-epoch-snapshot-dev-ceec-f0a329a-dirty.json` is 2,496,418 bytes with
SHA256 `D1AC92DB408D3AF48C0D7DCECF004C3C7426F46F2CF57FDD8A8908172850E023`;
its exclusive reservation SHA256 is
`AE69A6141FD21981840A41883D430A62D6C9F9429455347E776015B54B27754A`.
The runner's independent raw verdict is **FAIL** with no integrity errors:
exactly 54,000 successes/server requests/raw records, 2,200 clean boundaries,
and zero duplicate, missing, unexpected, cross-request, or cross-generation
completions.

Adjacent C0/E1 throughput ratios were sequential `1.025462`, saturated
`1.032009`, and no-backlog `1.056316`; p99 deltas were respectively
`-0.2200ms`, `-4.2848ms`, and `-0.3085ms`. The second C3/E2 block reversed for
the two required throughput cases: sequential `0.977727` and saturated
`0.984039`, with p99 regressions of `+0.0486ms` and `+4.0503ms`. No-backlog
remained favorable at throughput `1.038782` and p99 `-0.3293ms`, but the frozen
rule requires every workload and metric to pass both adjacent blocks.

The exact-epoch snapshot candidate is therefore permanently rejected and may
not be resampled standalone. Its production changes, deterministic candidate
tests, and temporary runner were removed. The raw artifact, reservation, red
baseline, correction/review evidence, declaration, and rejection remain. No
formal FIFO-v2 campaign was run.

After removal, the Pool/lifecycle four-file matrix passed **177 tests in
10.20s**, and the Actor/Pool/lifecycle six-file matrix passed **291 tests in
13.40s**. The lower counts are the expected removal of candidate-only
exact-epoch regressions; all retained production and test files match
`f0a329a` again.

## Post-`89d6439` State-Only IdentityGate Red Baseline

The next materially different candidate targets the repeated entrance
Condition in `IdentityGate.acquire_token()`. The Condition does not protect
owner, waiter, grant, terminal, or handoff state and is never used by release;
all of those invariants already linearize under `_state_lock` and each waiter
owns its exact Event. To preserve injected/legacy behavior, default
`IdentityGate()` and direct `_RequestGate()` remain on the existing Condition
plus state-lock path. Only Actor runtime control gates and Socket request/
submission gates opt in to `state_only=True`, where initial owner assignment or
waiter registration uses `_state_lock` directly and the existing waiter loop,
handoff, timeout, exception, and exact-token release paths remain unchanged.

Before production edits, deterministic opt-in, condition-independence, FIFO,
and production-construction nodes were added:

```powershell
python -m pytest -q tests/test_transport_actor_regressions.py::test_state_only_identity_gate_uncontended_acquire_skips_condition tests/test_transport_actor_regressions.py::test_state_only_identity_gate_is_independent_of_legacy_condition_owner tests/test_transport_actor_regressions.py::test_state_only_identity_gate_preserves_registered_waiter_fifo tests/test_transport_actor_regressions.py::test_production_actor_and_socket_gates_use_state_only_registration --tb=short
```

Result: **7 failed in 1.17s**. Both gate types rejected the new private option,
and production gates exposed no state-only mode. Existing default Condition
interrupt/release-independence, waiter FIFO, deadline, failed Event wakeup,
stale-token ABA, STOP/cancel, and close tests remain mandatory safety gates.

The implementation adds a private `state_only` option while preserving the
real Condition object and legacy default. State-only acquisition skips only
the initial Condition ownership; it takes the existing state lock, either
publishes the exact owner or appends the exact waiter, and then uses the shared
wait/handoff/withdraw logic. Actor runtime control gates and Socket request/
submission gates explicitly opt in; direct `IdentityGate()` and
`_RequestGate()` construction remain legacy. Release remains Condition-
independent, and FIFO begins at state-lock registration rather than call entry.

The focused IdentityGate/state-only matrix passed **37 tests in 1.52s**. The
Actor/Pool/lifecycle six-file matrix passed **306 tests in 13.38s**. Two fresh
current-byte read-only reviews are **CLEAN**: one ran all Actor regressions
(`100 passed`) plus Actor/lifecycle (`175 passed`) and verified STOP, cancel,
close, stale release, failed wake, deadline, and state-lock interruption paths;
the other repeated both state-only gate types for 100 two-waiter FIFO/handoff
rounds with every thread joined and owner/waiter state empty. Legacy Condition
interrupt/compatibility/release tests were not weakened.

## State-Only IdentityGate Development Diagnostic Declaration

This is one isolated development decision aid, not a formal acceptance
campaign and not a rerun or control sample of any frozen exact source or prior
campaign. Both roles run the same dirty candidate production bytes. Control
replaces only `IdentityGate.acquire_token()` in-process with its exact
`89d6439` legacy Condition-plus-state implementation; experiment restores the
candidate method. Construction, release/handoff, Actor/socket/pool code,
server, workload, and all other functions remain identical.
The runner additionally installs identical cell-boundary-only tracking wrappers
around benchmark Transport/Server/Executor classes; they record post-close
Broker, Actor runtime, server worker/connection, and executor queue/thread
snapshots without adding per-request instrumentation, and are included in the
same control and experiment bytes/process.

Frozen identities are actor SHA256
`3C6CA515D7066B38BA0CDE6CC7C464989F331C46F229937683A583874587F856`,
socket SHA256
`EAE6281408B2D2D0B46550B1254AE73869847191B7BB883840F705CC96FA6A48`,
workload SHA256
`B09AB7130752AE0C562B63BA04D2B1BEA42F1E168C060F13D6E86E9BBA277B84`,
and one-use diagnostic SHA256
`DDCC80505126EF1CC19E2B276B2F3B80CBD74D35B5D79024C1B9963A9C1EA2B9`.
The target and exclusive reservation
`C:\Users\ax\Desktop\eltdx\artifacts\state-only-identity-gate-dev-ceec-89d6439-dirty.json`
and `.json.reserved` companion did not exist at declaration time. Execution
must pass the runner hash in `ELTDX_STATE_ONLY_DIAGNOSTIC_SHA256`; all four
hashes are verified before reservation, before and after every cell, and at
completion.

Run exactly one C/E/E/C process with no overlapping task pytest, stress, or
benchmark process. Every fresh-loopback cell runs 1,500 sequential requests
after 300 warmups, 10,000 pool-4/concurrency-100 requests after 500 warmups,
and 500 pool-4/four-worker cohorts after 50 warmup cohorts, all at 5ms server
delay. Retain every raw latency and completion record. Require exactly 54,000
successes/server requests/raw records, 2,200 clean cohort boundaries,
`cleanup_complete=true` for every cell, unique token/provenance/digest
agreement, and zero duplicate/missing/unexpected/cross-request/cross-
generation counts. Exclusive reservation precedes sampling; active-cell state
is written before each cell and every update atomically replaces the artifact.
Cleanup evidence is a real post-close snapshot: each pool Broker is closed
with zero active leases/waiters/pin-waiters; every captured Actor runtime is
STOPPED with no fatal/cleanup error, cancel request, selector registration,
generation/socket, wake fd, pending/active ticket, or live thread; each slot has
no runtime/candidate/unpublished candidate; every server worker/connection and
executor thread/queue is empty.

The retain rule is frozen before sampling and uses raw integers only. Compare
C0/E1 and C3/E2. Throughput passes when
`experiment_requests * control_elapsed_ns >= control_requests * experiment_elapsed_ns`;
p99 is `sorted(latency_ns)[((n - 1) * 99) // 100]` and passes only when the
experiment value is no greater than control. Sequential, saturated, and no-
backlog must pass both metrics in both adjacent blocks. Any metric reversal,
integrity/source mismatch, or incomplete cell permanently rejects the
candidate; it may not be resampled standalone. Passing only permits full
verification and a brand-new formal FIFO-v2 campaign.

## State-Only IdentityGate Diagnostic Rejection

The declared state-only diagnostic was executed once with the frozen C/E/E/C
schedule. It stopped after C0 completed its workload because the runner's
cleanup predicate rejected a valid post-close snapshot: Actor runtimes were
STOPPED with no live threads, sockets, generations, wake fds, tickets,
requests, fatal errors, or cleanup errors; Brokers had no active leases or
waiters; servers had no workers or connections; and executor threads had
exited. The only false-positive fields were selector maps retained by closed
selector objects and executor queues retaining their normal shutdown
sentinel. No performance cell was retained and no performance verdict exists.

Artifact:
`C:\\Users\\ax\\Desktop\\eltdx\\artifacts\\state-only-identity-gate-dev-ceec-89d6439-dirty.json`
SHA256 `D267549545DADBC7014A96D733F316FBC3CEB0EC5FE89353C06F57D1CCF24EE8`;
exclusive reservation SHA256
`078A1369D170F7D5C8EB2D6718C6306240E010B65DD87C51B028AF14055D37FA`.
The final runner SHA used by the one-use declaration was
`DDCC80505126EF1CC19E2B276B2F3B80CBD74D35B5D79024C1B9963A9C1EA2B9`.
Because the cell was incomplete under the predeclared rule, this candidate is
permanently rejected and must not be resampled standalone. The candidate
production and test changes, plus the temporary runner, are being removed;
the raw failed artifact and reservation remain as audit evidence. The
user-modified `ACTOR_REFACTOR_RESULT.md` is intentionally untouched.

After removing the rejected state-only candidate and its temporary runner, the
full retained Actor/Pool/lifecycle six-file matrix was rerun:

```powershell
python -m pytest -q tests/test_transport_actor_regressions.py tests/test_transport_actor.py tests/test_transport_pool_regressions.py tests/test_transport_pool.py tests/test_transport_lifecycle_regressions.py tests/test_transport_lifecycle.py --tb=short
```

Result: **291 passed in 13.21s**. Actor, Socket, and regression test bytes are
back to the `89d6439` retained baseline. No state-only performance verdict
exists; the next candidate must be a new source/campaign and cannot reuse the
frozen exact-epoch or state-only diagnostic declarations.

## Exact `792b3db` Remote Checks

Draft PR #12 remained open and draft at exact head
`792b3db31772bfd8d7607fc23c954cbb02e4c7d8`. Pages run `29523075367`
completed successfully. CI run `29523075436` completed successfully for
Ubuntu Python 3.10, 3.11, 3.12, and 3.13, Windows Actor Python 3.11 and 3.13,
and package build. This proves the rejected state-only cleanup checkpoint is
portable; it does not close the still-open F06 performance requirement.

## Post-`792b3db` Wakeup Armed-Bit Red Baseline

The next materially different candidate coalesces only Actor wakeup bytes.
Every pending request, cancel, and STOP is still published under the exact
runtime control gate. That publication also claims one `wake_armed` bit. The
first publisher sends one non-blocking socketpair byte outside the gate;
subsequent publishers while that byte remains unconsumed still signal
`control_ready` but do not repeat the physical send. The Actor clears the bit
under the same control gate only after `_drain_wakeup()` has consumed through
`BlockingIOError`, then immediately drains control as before. A publication
that linearizes before the clear is therefore visible to that control drain;
one after the clear claims and sends a new byte. Notify failure clears the
claim, while `abandon_actor()` retains an unconditional best-effort physical
wakeup. No receive, TCP, FIFO, Broker, lease, retry, or deadline behavior is
changed.

Deterministic red tests cover physical-send coalescing, drain-to-EAGAIN rearm,
publication during drain without a lost request, cancel and STOP visibility
behind an existing wake byte, notify failure rollback, and the finalizer's
unconditional fallback. Existing writer-full, writer-EOF, explicit-writer,
successor-grace, ticket terminalization, and control-lock interruption tests
remain mandatory controls.

Red command:

```powershell
python -m pytest -q tests/test_transport_actor_regressions.py -k "wakeup_armed or wakeup_coalesces_while or wakeup_drain_to_empty or wakeup_drain_observes or existing_wakeup_keeps or notify_error_rolls_back or finalizer_forces_wakeup" --tb=short
```

Result before production edits: **6 failed, 2 passed, 85 deselected in
1.04s**. The current runtime has no armed state, two notifications write two
bytes, drain does not rearm, request/cancel/STOP each repeat a write behind an
existing byte. Notify-error propagation and the finalizer's unconditional
write are the two retained green controls.

The implementation made all eight focused nodes pass and the Actor/Pool/
lifecycle six-file matrix passed **299 tests in 12.75s**. During review, the
first implementation also exposed and fixed a finalizer regression: the
forced explicit-writer path unnecessarily reacquired `control_lock` and could
block behind successor-grace clearing. The corrected finalizer race passed 20
consecutive rounds, and the Actor two-file matrix then passed **122 tests in
3.82s** without warnings.

The candidate is nevertheless rejected before performance sampling. A normal
request's wake byte is drained and `wake_armed` is cleared before the Actor
accepts and sends that ticket. The next pooled lease cannot submit until the
previous ticket's terminal completion releases the lease, so sequential,
saturated, and fixed-cohort steady paths still perform one physical socketpair
write per request. The candidate would add one Actor-side control-gate acquire
per request and coalesce only rare submit-then-cancel/STOP bursts. It therefore
cannot improve the measured publication-to-send path and has a structurally
negative steady-state cost. Independent read-only review agreed: **do not
sample; remove the candidate**. No one-use diagnostic declaration, reservation,
or artifact was created. Production changes and experimental tests are being
removed; the red baseline and design rejection remain as audit evidence.

A second independent adversarial review found an additional correctness reason
to reject the candidate. The tentative armed claim was visible before the
physical send completed. A concurrent cancel or STOP could depend on that
claim and skip its send; if the first send then raised `OSError`, returned a
short write, or failed while signaling successor grace, rollback cleared the
bit without retransmitting the coalesced control work. In the STOP case the
first notify could also suppress its error after observing `stop_requested`,
leaving the Actor blocked in `selector.select()` until close timed out. A
safe in-flight generation/retry protocol would add more state to an already
structurally negative steady path. Full removal is therefore authoritative.

After removing all armed-bit production and experimental-test bytes, the
retained Actor/Pool/lifecycle six-file matrix passed **291 tests in 13.05s**.
Actor and regression test files again match the `792b3db` retained baseline;
only this progress ledger and the pre-existing user-owned result-document edit
remain modified.
