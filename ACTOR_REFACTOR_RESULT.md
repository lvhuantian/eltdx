# Actor Transport Refactor Correction Result

This is the permanent recovery and audit record for the eltdx 7709 Actor
transport refactor. It explicitly supersedes the invalid COMPLETE claim at
`994c49b` and all earlier 183-test acceptance evidence. The correction cycle is
complete only at `SELF`, after the exact-HEAD CI and Pages checks described
below succeed.

## Delivery Identity

| Field | Value |
| --- | --- |
| Status | FINAL candidate; resolves to COMPLETE only after `SELF` exact-HEAD checks succeed |
| Authoritative spec | `ACTOR_REFACTOR_PLAN.md`, revision 1.3 |
| Spec SHA256 | `C38A3791C4C0B44677325797110BD283AB0D0580E103952C2F2DEAD6839618B2` |
| Performance authorization spec commit | `5ff6447d2acaa04ab8c406970c2a6b81e8ccd94f` (revision 1.2) |
| Final procedural spec commit | `SPEC-CANDIDATE`, replaced with its exact SHA before FINAL |
| Refactor base | `71089c0a2867a75dc79aa2c340213f4e3845b6e3` |
| Overturned acceptance | `994c49b51f47255bdcd9cdc3308a5a554f37588b` |
| Corrected implementation | `3287b6a775e6c9fe7a0bcecfe134fc94b6d6634d` |
| Final evidence checkpoint | `e94f9cda179d10be9a8f49f7cbafff9e3ea7ec66` |
| Final manifest commit | `SELF`, defined below |
| Branch | `actor-transport-refactor` |
| Draft PR | [#12](https://github.com/electkismet/eltdx/pull/12), OPEN, draft, unmerged |
| Latest evidence CI | [run 29551509105](https://github.com/electkismet/eltdx/actions/runs/29551509105), SUCCESS |
| Latest evidence Pages | [run 29551509107](https://github.com/electkismet/eltdx/actions/runs/29551509107), strict build SUCCESS |

`SELF` is the newest first-parent commit containing this manifest, with no
`ACTOR_REFACTOR_FIX_PROGRESS.md`, and with trailer `Fix-Checkpoint: FINAL`:

```powershell
git log -1 --first-parent --format="%H%x09%B" --grep="Fix-Checkpoint: FINAL"
```

A commit cannot embed its own SHA. At delivery, local HEAD, the remote branch,
and PR head must all equal `SELF`. The FINAL CI and Pages runs are recovered
with `gh run list --commit SELF`; their exact URLs and conclusions are reported
after they finish.

## Correction Checkpoints

| Phase | Commits in first-parent order | Result |
| --- | --- | --- |
| Original A00/A01 | `f016868`, `0a1f034`, `79a14f9` | Baseline, remote synchronization, deterministic Actor infrastructure |
| Original A02/A03 | `20387d6`, `608fdeb` | Incremental decoder and nonblocking Actor runtime |
| Original A04/A05 | `949c787`, `5f82000` | Wire lifecycle and SocketTransport facade |
| Original A06/A07 | `e7d8fca`, `049f101` | FIFO pool leases, pin proxies, lifecycle and finalizers |
| Original A08/A09 | `bf6fed2`, `9755ee5`, `c2c4eb0`, `b30c6b9`, `c403681`, `994c49b` | Stress/platform/docs, decoded budgets, heartbeat guard and overturned final evidence |
| Correction F00-F06 | `05a5e9b`, `8aa089d`, `2e48be0`, `117b8c6`, `0955a8e`, `cc46e60` | Baseline ledger through resource-sampling correction |
| Final lifecycle review | `0b8ad54`, `b66a7a8` | Shutdown, startup, finalizer and post-review cleanup gaps |
| FIFO-v1 diagnosis | `dcf6190`, `2058ee6`, `359bb47`, `11be931`, `ca43972`, `8303405` | Hot path, evidence conflict, contract diagnosis, frozen campaign and retained FAIL |
| Post-campaign correctness | `7b961fe`, `a53cc09` | Partial-send receive order, connect cancel, lifecycle interruption and deterministic generation failover |
| FIFO-v2 low-risk cycle | `66e4496`, `72ef660`, `8296511`, `d76ca47`, `2da7651`, `d9619b0` | Campaign freeze, Windows deadline, low-risk hot path, remote checks and retained FAIL |
| Windows/pool experiments | `052ff68`, `76c3a95`, `e106ad4`, `0183c49`, `9338286`, `2d4cea8` | Rejected lock/return candidates, Actor cooperation, pool hot path and evidence |
| Post-hotpath correctness | `3201d3d`, `907e3e6` | Actor/pin race fixes and exact correction checks |
| Revision-7/final campaign | `f7355c0`, `7923287`, `7e78bca` | Heartbeat/DNS evidence, revision-7 checks and immutable FIFO-v2 FAIL |
| Rejected successor candidates | `29b250e`, `f0a329a`, `89d6439`, `792b3db`, `e9d2c8c`, `2a4e396` | Wake, pre-send, snapshot, diagnostic and Broker candidates rejected without favorable resampling |
| Exception/control/final evidence | `eacbfc0`, `e455234`, `5ff6447`, `3287b6a`, `e94f9cd` | Exact heavy evidence, blocker audit, revision 1.2 authorization, control priority and exact final-source evidence |
| FINAL | `SELF` | Permanent evidence, clean reviews, ledger removal, exact-HEAD verification |

This table covers every commit from the original A00-A09 implementation and
every correction-cycle commit from `994c49b` through `e94f9cd`. All were
appended and pushed normally; no published commit was amended, rebased or
force-pushed.

## Correctness Corrections

### Frame and request boundary

- Every received frame has a monotonically increasing receive sequence. Each
  wire exchange captures its receive boundary, and a response must be newer
  than that boundary and fully sent before it can match.
- STOP and exact cancel are drained before work. Decoded frames and the socket
  receive batch are classified before the next wire exchange starts, including
  batches larger than the 64-frame fairness budget and handshake-batch tails.
- Decoder continuation uses one ProtocolError/OSError/EOF boundary. A partial
  response at EOF is a retryable connection close; complete malformed protocol
  data remains a non-retryable ProtocolError.
- Old/future-ID poison frames become bounded push data and cannot complete the
  next request.

### Ticket, cancel, build, and deadline

- Connect and request tickets have exact monotonic request IDs plus runtime and
  lease identity. Late standalone and reused pinned-lease cancellation is a
  no-op unless the exact active/pending ticket matches.
- Wake notification failure atomically withdraws and terminalizes the exact
  pending ticket.
- Unsupported command, invalid market, negative start, and other frame-build
  errors fail only that ticket. The Actor and pool remain usable.
- Ticket waits use the caller's absolute monotonic deadline without fixed
  50ms/100ms grace. Endpoint candidates and one retry share that deadline.

### Control priority and terminal ownership

- Heartbeat admission calls the Broker guard outside the Actor control lock,
  then atomically rechecks STOP, active/pending work, exact cancels, generation,
  interval and activity before publishing an internal ticket.
- Every new TCP generation and every wire send has an exact control-lock claim.
  If STOP or the exact `(runtime_epoch, request_id, lease_id)` cancel wins first,
  no socket creation, retry progression or wire send begins.
- Decoded terminal responses, non-terminal handshake phase transitions,
  ConnectTicket success and all final failure paths linearize on the same
  control lock. `terminal_claimed` gives one completion owner; a later cancel is
  a no-op and cannot leave a stale token.
- Heartbeat and handshake payloads are parsed before terminal success is
  claimed. Malformed input fails its ticket, releases the request owner and
  leaves the Actor usable for a later valid request.

### Endpoint failover and Windows

- Handshake failure, partial send, EOF, partial-frame EOF, response timeout,
  and retry rotate from the failed endpoint to the next endpoint instead of
  resetting to index zero.
- Windows READ/WRITE completion verifies `SO_ERROR` and peer identity. Errno
  sets include platform values such as WSAEINTR without hard-coded platform
  assumptions.
- An anomalous ready socket whose `SO_ERROR` remains in progress is unregistered
  and selector-rearmed at a bounded 10ms probe interval, not busy-polled.
- Real two-loopback tests cover refused first address, handshake EOF, partial
  business response EOF, response failure, healthy backup, and one deadline.

### Broker, pin, and lifecycle

- Every admission has a fresh Event. Reservations are decremented before wake,
  eliminating Event ABA and double-decrement races while preserving FIFO.
- Pinned connect/execute are pin-local admitted operations. Pinned close is a
  monotonic OPEN/CLOSING/FAILED/CLOSED state machine and never releases a slot
  before its exact pending/active wire ticket is terminal.
- Actor candidates are installed before `Thread.start()`. Startup errors retain
  the exact runtime, and startup cleanup remains owned until every stage ends.
- Mailbox submission and pool epoch retirement share a submission gate. The
  runtime guard validates exact broker/epoch identity and seals before stop.
- Concurrent close and FIRST_EXCEPTION rollback share one identity-bound
  `ShutdownAttempt`. Old attempts cannot stop a reopened epoch, failure states
  are monotonic, and all closers observe the same result.

## Compatibility and Scope

Preserved public behavior:

- `TdxClient`, `SocketTransport`, `PooledSocketTransport`, context managers,
  all existing 7709 business methods, automatic handshake, push APIs,
  `connected_host`, `last_handshake`, `last_heartbeat`, and diagnostics.
- `pool_size=N` still creates exactly N Actor slots/connections, independent of
  the host count. No process-global worker pool or per-host thread expansion was
  introduced.
- Caller-side business parsing remains outside the Actor and releases a normal
  lease at wire terminal.

No 7615 source, business command API, `main`, tag, release, PyPI state, or
unrelated Pages content changed. Runtime dependencies remain empty.

Intentional compatible extensions remain bounded defaults:

| Parameter | Default |
| --- | ---: |
| `max_pending_requests` | `256` |
| `push_queue_size` | `1024` |
| `push_queue_bytes` | `8,388,608` |

## Capacity Evidence

| Structure | Observed maximum | Configured hard limit | Evidence |
| --- | ---: | ---: | --- |
| Actor business mailbox | 1 active wire ticket | 1 | deterministic control-priority and submission-gate regressions reject a second active exchange |
| Pool admission | 3 FIFO waiters | 4 in the deterministic test; 256 by default and in heavy stress | `test_lease_broker_assigns_waiters_fifo_and_releases_exactly_once` observes waiter depths 1, 2 and 3 in order, then zero |
| Active normal leases | 4 | `pool_size=4` | 100,000-request heavy workload records server maximum active exactly 4 and zero leases after close |
| Push frames | 1,024 | 1,024 | exact-final heavy artifact records `max_frames_observed=1024` |
| Push wire bytes | 28,612 | 8,388,608 | exact-final heavy artifact records `max_bytes_observed=28612` |
| Incremental RX bytes | at most 32 | 32 in the bounded regression; 65,551 in production | 1,000-byte garbage feed asserts `max_buffer_observed <= 32` |
| Resynchronization discard | exactly 1,000 | 1,000 in the bounded regression; 65,536 in production | the same feed reaches the configured discard boundary without retaining unbounded input |
| Decoded-frame queue | 910 appended; 846 retained after the immediate 64-frame fairness slice | 1,024 | the 1,101-frame legal burst uses 18-byte frames and a 16,384-byte capped recv; an exact-source tracking probe observed 910/846 and the regression drains through the matching response without loss |
| Frame/decompressed payload | 65,535 bytes | 65,535 bytes | declared, compressed, output and trailing-data boundary tests pass |

All final snapshots have zero pending/active tickets, cancel tokens, admission
waiters, pin waiters and leases. Push frames and bytes are zero after close.

## Deterministic Regression Evidence

The original A-E corrections were made red against `994c49b` before their
corresponding fixes. They cover stale decoded and handshake-batch frames,
greater-than-64 continuation, EOF and partial EOF, colliding IDs, late cancel,
request-local build errors, absolute deadlines, multi-endpoint rotation,
Windows refused-first connect, Event ABA, pin close/capacity, fatal late
registration, concurrent start/close and monotonic failed shutdown.

Post-campaign review added deterministic red tests for partial-send receive
ordering, cancellation while connecting, interruption cleanup, generation
failover, Broker reclaim and lifecycle publication. Post-authorization review
then made the following old direct paths fail deterministically before
`3287b6a`: heartbeat admission after a business/control winner, new generation
start after cancel/STOP, decoded terminal response after cancel/STOP,
handshake-phase advance after cancel/STOP, ConnectTicket success after
cancel/STOP, deadline/build/final failure after the winning control and stale
cancel after terminal claim.

Three parameters are retained as intermediate-fix regressions rather than
misrepresented as old-HEAD failures: malformed heartbeat, request-retry plus
cancel, and request-final plus cancel already passed at `5ff6447`. They protect
the final terminal-claim ordering introduced later.

Final local commands on production source `3287b6a`:

| Command | Result |
| --- | --- |
| Control-priority focused selection | 11 passed, 100 deselected in 0.51s |
| `tests/test_transport_actor_regressions.py` | 111 passed in 3.56s |
| `python -m pytest -q` | 547 passed in 250.14s |
| Heartbeat hard-gate node | PASS in 209.30s |
| `python -m build` | sdist and wheel built successfully |
| `python -m mkdocs build --strict` | PASS in 3.64s |
| `python -m compileall -q src tests scripts` | PASS |

Package SHA256 values are
`04F3457FF0032E244A50728D1BEE42B9AC5B9E4A131BF471C004E12A1CCBE9A5`
for the sdist and
`0709D124B9055BC2AEBDFFB8F067DA16C522C03666F5F0866460AD19FED960A6`
for the wheel.

## Stress, Ownership, and Resources

Exact clean final-source command:

```powershell
python scripts/stress_actor_transport.py --generations 10000 --requests 100000 --pool-size 4 --concurrency 100 --close-samples 100 --heartbeat-requests 1000 --idle-seconds 0.5 --resource-rounds 8 --resource-warmup 3 --resource-generations 50 --output artifacts/actor_stress_final_3287b6a.json
```

The Windows 11 / Python 3.12.6 process passed in 185.4s. The 736,903-byte
artifact SHA256 is
`224872904E29C3905C55087656B38155586BB8CFBEAEAE5F5E1333693724176F`;
workload SHA256 is
`F7E187E3960002FBF0194C686182C3676152EBA7C6FD68AB4BC46EDE8262E5B1`.
It records exact implementation
`3287b6a775e6c9fe7a0bcecfe134fc94b6d6634d` and
`worktree_dirty=false`.

Every logical request uses the existing retry-safe file-content command with a
unique uint32 nonce. The server echoes that nonce, real server ID, connection
ID, and wire-attempt sequence. Two real listeners share the provenance ledger
and inject future-ID poison frames, partial EOF, reconnects, push, and delay.

| Workload | Raw result |
| --- | --- |
| 10,000 generations | 24.813711s, 403.003 rps, exact Runtime/Actor/Thread identity unchanged, accepts 5,000/5,000 |
| Generation ownership | 15,000 attempts, 5,000 cross-endpoint retries, 10,000 unique; duplicate/missing/unexpected/cross-request/cross-generation/stale all 0 |
| 100,000 mixed | 93.119775s, 1,073.886 rps, 100,043 attempts, 43 real cross-endpoint retries, server traffic 21,500/78,543 |
| Mixed concurrency | Server maximum active exactly 4; accepts 43/82 and Actor generations 32/23/38/32 both total 125 |
| Mixed ownership | 100,000 unique; duplicate/missing/unexpected/cross-request/cross-generation all 0 |
| Push pressure | 2,044 push/poison frames, 1,020 bounded oldest drops, one explicit gap |
| Idle CPU | 0.502232s wall, 0 process CPU, ratio 0 |

After close, every retained runtime reports Actor dead, generation/selector/
wakeup/tickets absent, saved TCP/wakeup sockets closed, cancel map empty, and
state STOPPED. Broker waiters, pin waiters, and leases are zero and closed;
PushBuffer frames/bytes are zero and closed. Actor threads after both workloads
are zero.

After three identical warmup rounds, eight complete 50-generation rounds each
measured exactly 201 Windows handles after the workload returned and GC
completed:

```text
201, 201, 201, 201, 201, 201, 201, 201
```

Every round also had zero Actor threads, exact owned-resource cleanup, and zero
cross-request/cross-generation completion. There is no tolerance and no
monotonic growth.

Close latency over 100 samples per condition:

| Condition | p50 ms | p95 ms | p99 ms | Max ms | Gate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Idle | 3.0985 | 3.9851 | 5.7324 | 8.3010 | p99 < 100 |
| Loaded | 2.4216 | 2.9370 | 4.6375 | 4.8599 | p99 < 250 |

All 400 loaded futures terminalized with the expected close error, every
ticket was terminal, and all 800 retained Actor-owned resource snapshots were
closed. Broker waiters, pin waiters and leases were zero; PushBuffer frames and
bytes were zero and closed. An independent raw-artifact audit recomputed the
artifact hash and reconciled every server, retry, close, heartbeat and resource
counter without a finding.

## Heartbeat Evidence

The canonical revision-7 formal artifact SHA256 is
`985A800AE0AD12463F9EE21018FA180AACF901FE6E63D58D9E5667E1F7761C9E`.
It records implementation `907e3e69...` with `worktree_dirty=true`; it is the
retained predeclared formal campaign, not an exact-final-source artifact.
It retains all 32 balanced phases, 260 configuration barriers and 131,232
unique business responses. Timed heartbeat was `0/0`, duplicate/missing/
unexpected/cross-request/cross-generation and generation/accept fence
mismatches were all zero. Aggregate enabled/baseline throughput ratio was
**0.998163**, impact 0.1837%, below the strict 1% limit.

The clean exact-final-source `3287b6a` heavy artifact provides final-source
applicability and independently produced ratio **0.998884**,
impact 0.1116%, with block ratios
`0.998003/1.004117/1.020108/0.974237`, median block ratio `1.001060`,
35,232 unique responses and all error/cross counters zero. It proves four idle
connection heartbeats and 32 paced heartbeats, with
`heartbeat_during_business=0` and idle CPU ratio zero.

One earlier local full-suite sample measured `0.989062` and failed the strict
`>0.99` node; it is retained as a failed sample, not reclassified. The node was
rerun in isolation and passed in 209.30s, then the stable final local suite and
exact CI matrix passed. The formal revision-7 artifact remains the canonical
heartbeat acceptance record.

## Performance Evidence

The authoritative frozen campaign is `fifo-v2-7923287-a`, comparing clean base
`71089c0a2867a75dc79aa2c340213f4e3845b6e3` with clean Actor source
`79232870c337a94e5d79eca723d8bf5d09371e89`. It ran all eight predeclared
attempt-1 cells once in fixed
`baseline/current/current/baseline/current/baseline/baseline/current` order.
No result was discarded, replaced or resampled.

| Evidence | SHA256 |
| --- | --- |
| Canonical declaration | `5ab6e75cd12d71e396c09ee592a174b7c4900be69ce606b02527e609428a6cde` |
| Declaration file | `36ff99846b8a300f738b85abc20680686a462e07f4b79161779cf44ff9dbd484` |
| Immutable bundle | `2497cf1e3efe07e449511935661da249238b4443d2a9bae906bebe0ed8373961` |
| Stored report | `954192977dee7699dfd1c8991e0dcf2694fa8a3047ab0174fb93849083ead4d1` |
| Independent replay | `15339fd279e6330672553a7aa53d18498de6666213ddbd0dfa2549505d328b7f` |

Across 32 cases, all 1,000,000 requests, successes, server requests, wire
attempts, unique responses, completion rows and raw latency rows matched
exactly. Duplicate, missing, unexpected, cross-request, cross-generation,
digest and provenance mismatches were zero. Current fixed cohorts completed
10,640 clean boundary checks. Artifact audit and verifier replay were CLEAN,
with `errors=[]`.

The immutable verifier result is **FAIL**:

| Gate | Baseline | Actor | Result |
| --- | ---: | ---: | --- |
| Sequential throughput | 174.410898 rps | 163.773610 rps | ratio `0.939010`, below 0.95 |
| Saturated throughput | 687.407879 rps | 634.593945 rps | ratio `0.923169`, below 0.95 |
| Sequential p50 | 5.6370ms | 6.0193ms | delta 0.3823ms, allowance 0.5637ms, PASS |
| Sequential p99 | 6.1812ms | 6.8017ms | delta 0.6205ms, allowance 0.61812ms, FAIL by 0.00238ms |
| No-backlog p50 | 6.0721ms | 6.6680ms | delta 0.5959ms, allowance 0.60721ms, PASS |
| No-backlog p99 | 6.8170ms | 8.4244ms | delta 1.6074ms, allowance 0.6817ms, FAIL by 0.9257ms |

Saturated raw p50/p99 was baseline `132.56405/536.1256ms` versus Actor
`155.13345/170.9080ms`. The report-only contended-wave p50/p99 was baseline
`78.0597/150.6717ms` versus Actor `89.90885/168.4938ms`. These diagnostics do
not offset any failed gate.

On 2026-07-17 the user selected the preserve-ownership option: keep
Actor-exclusive socket ownership, strict FIFO, synchronous APIs and exactly
`pool_size=N` Actors/connections, with no caller-side direct send. Plan
revision 1.2 authorizes the four disclosed old-implementation comparison
failures as a one-delivery architecture exception. The campaign, thresholds,
raw values and FAIL verdict are unchanged; the exception changes only FINAL
completion policy. Concurrency=N, heartbeat, close, idle CPU, uniqueness, cross
counters and resource cleanup remain hard gates with no exception.

This campaign will not be rerun or reclassified. Future performance-sensitive
changes use the FINAL Actor source as a prospective baseline under a rule frozen
before sampling; this exception cannot hide a future regression.

## Cross-Platform CI and Builds

Exact evidence checkpoint `e94f9cda179d10be9a8f49f7cbafff9e3ea7ec66`
run [29551509105](https://github.com/electkismet/eltdx/actions/runs/29551509105):

| Platform | Python | Result |
| --- | --- | --- |
| Ubuntu | 3.10 | 546 passed, 1 Windows-only skip, SUCCESS |
| Ubuntu | 3.11 | 546 passed, 1 Windows-only skip, SUCCESS |
| Ubuntu | 3.12 | 546 passed, 1 Windows-only skip, SUCCESS |
| Ubuntu | 3.13 | 546 passed, 1 Windows-only skip, wheel/sdist build SUCCESS |
| Windows | 3.11 | Full suite, 547 passed, SUCCESS |
| Windows | 3.13 | Full suite, 547 passed, SUCCESS |
| Pages | [run 29551509107](https://github.com/electkismet/eltdx/actions/runs/29551509107) | strict build and artifact upload SUCCESS |

The Windows jobs run the full suite, including all correction regression files
and the real Windows refused-first `connect_ex` test. Pages deployment remains
intentionally skipped for a pull request; the site build is the required gate.

The test-marker audit found one conditional skip only:
`test_windows_refused_first_address_reaches_healthy_backup`, guarded by
`sys.platform != "win32"`. It accounts for the single Ubuntu skip and runs on
both Windows jobs. Windows has zero skipped tests. There are no xfail or flaky
markers, plugins, rerun rules, or intermittent-failure allowlists masking a
failure.

The `71089c0..e94f9cd` scope review covers all 39 changed files. Project runtime
dependencies remain exactly `dependencies = []`; no 7615 implementation or
business-command facade was changed. The `client.py`, host and registry edits
are bounded 7709 transport integration/validation changes, while the two MCP
test edits only prevent their context-manager fixtures from opening a real
7709 connection. Workflow changes add the required Windows matrix; MkDocs and
Pages changes are limited to Actor architecture, API, debugging and changelog
content. No unrelated Pages content, release metadata or publication path was
added. An independent scope/CI reviewer reached the same conclusion.

## Review and Completion Audit

- Two independent control-path reviews found the heartbeat, connect and
  terminal-ownership gaps after the performance exception. Every finding was
  reproduced, fixed and rereviewed CLEAN against `3287b6a`; implementation,
  lock order, retry/failure behavior and regression mapping are clean.
- An independent audit of the exact final heavy artifact recomputed its SHA and
  reconciled all server, retry, close, heartbeat and resource counters CLEAN.
- Two fresh read-only FINAL manifest/code/scope reviews are required after this
  document is complete. Any finding is fixed rather than recorded as an
  accepted risk.
- The FINAL commit must have no progress ledger, no untracked non-ignored file,
  no task-owned process, and two or more clean independent review conclusions.
- Local HEAD, remote branch, PR head, FINAL CI, and FINAL Pages must all resolve
  to `SELF` before completion is claimed.

## Remaining Documented Limits

- Standard-library DNS for a previously unseen custom hostname is a documented
  caller-side preflight and cannot be cancelled reliably. It owns no Actor,
  lease, or TCP resource; numeric/cached endpoints use the full deadline.
- Finalizers are best-effort non-blocking stop/wakeup fallbacks. Deterministic
  release requires explicit `close()` or a context manager.
- Loopback performance isolates transport scheduling. Real exchange latency is
  outside this refactor and is not represented as a benchmark claim.

The PR remains intentionally unmerged and draft. No tag, release, main update,
or package publication was performed.

## Recovery Rule

Resolve `SELF`, verify branch/PR identity and exact-SHA checks, then use this
manifest and the Git history as the authority. Do not recreate the temporary
correction ledger after FINAL. If any later commit exists, audit it and its
checks before relying on this result.
