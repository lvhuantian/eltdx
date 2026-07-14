# Actor Transport Refactor Result

This is the permanent recovery and audit record for the eltdx 7709 Actor
transport refactor. It replaces the temporary `ACTOR_REFACTOR_PROGRESS.md`
ledger, which is deleted in the same finalization commit.

## Delivery Identity

| Field | Value |
| --- | --- |
| Status | COMPLETE after the latest FINAL-head CI verification |
| Spec | `ACTOR_REFACTOR_PLAN.md`, revision 1.0 |
| Spec SHA256 | `C13F9F551CDE202B48B3C1CD7307C2CD31B65DBBA255247D822A444B813CDF61` |
| Base SHA | `71089c0a2867a75dc79aa2c340213f4e3845b6e3` |
| Final verified code SHA | `b30c6b9ce885e29666f0e90a1565a195cf50074a` |
| Final local/remote SHA | `SELF`, defined below |
| Work branch | `actor-transport-refactor` |
| Draft PR | [#12](https://github.com/electkismet/eltdx/pull/12), OPEN, draft, unmerged |
| A09 CI | [run 29327373570](https://github.com/electkismet/eltdx/actions/runs/29327373570), SUCCESS |
| A09 Pages | [run 29327373632](https://github.com/electkismet/eltdx/actions/runs/29327373632), build SUCCESS |

`SELF` is the newest first-parent commit that contains this manifest, has no
`ACTOR_REFACTOR_PROGRESS.md`, and has the trailer `Actor-Checkpoint: FINAL`.
A Git commit cannot embed its own cryptographic SHA, so the exact value is
intentionally resolved from Git rather than represented by an impossible
self-hash:

```powershell
git log -1 --first-parent --format="%H%x09%B" --grep="Actor-Checkpoint: FINAL"
```

At delivery, local HEAD, `refs/heads/actor-transport-refactor`, and PR #12 head
must all equal `SELF`. The FINAL CI and Pages runs are the successful runs whose
`headSha` equals `SELF`; use `gh run list --commit SELF` to recover their URLs.
The delivery response records the resolved SHA and URLs after those runs finish.

## Checkpoints

| Checkpoint | Commit | Result |
| --- | --- | --- |
| A00 | `f0168688f8d1ac26f00291e69bb4717b3d3aed77` | Baseline, spec, branch, 102 passing tests |
| A00 sync | `0a1f03470cb680fbb58c2a661b7b113533c71fb0` | Draft PR and remote synchronization |
| A01 | `79a14f9ca9a36ae43e0f9c8b3af8df4a3a767ceb` | Deterministic support and legacy benchmark |
| A02 | `20387d691fce9c3dbd399e02897cdf83b5ef0020` | Incremental decoder and bounded zlib |
| A03 | `608fdeba1b4a01c7a7e43be254d226491029ccf3` | Non-blocking Actor runtime/connect/wakeup |
| A04 | `949c787ff020af22cc4f084c2382105423de65c8` | Wire lifecycle, identity, retry and cancel |
| A05 | `5f820004378ddb993714a8c8be61fc2d71e1425e` | Socket facade, Actor heartbeat and push |
| A06 | `e7d8fca000af9976a26f609406875e38ca204d52` | FIFO leases, real pin and pool rollback |
| A07 | `049f101559e2801ff04cac926c855f11bc8f9b99` | Lifecycle, fail-closed and finalizers |
| A08 | `bf6fed2f19f6a9155fa909b2799eea2e7d37e272` | Stress, performance and platform CI |
| A09 | `9755ee5cfbc9bba616479e166585c03ff6a92bf6` | Documentation and final review |
| A09 correction | `c2c4eb0135c4e05e11f72766faa33a83ea52df99` | Preserve decoded frames across fairness slices |
| A09 correction | `b30c6b9ce885e29666f0e90a1565a195cf50074a` | Defer pooled heartbeat under Broker pressure |
| FINAL attempt | `c4036811ed66085950ad27eaca0d783fa6b9294b` | Permanent manifest and ledger deletion; CI found a scripted-server close race |
| FINAL correction | `SELF` | Deterministic server lifetime fix and permanent manifest update |

All published checkpoint commits were appended and pushed normally. No pushed
commit was amended or rebased, and the branch was never force-pushed.

## Delivered Architecture

- Every pool slot owns at most one long-lived Actor thread, one non-blocking TCP
  socket, one selector, one socketpair wakeup, and one in-flight wire request.
- Only the Actor touches its TCP socket. It uses `connect_ex`, `SO_ERROR`,
  partial `send`, incremental `recv`, exact socket/generation selector tokens,
  and a generation-owned bounded decoded-frame queue.
- Runtime epoch, TCP generation, lease ID, message ID, message type and socket
  identity prevent stale events or responses from completing a new request.
- The caller performs business payload parsing after wire-terminal completion
  and normal-lease release.
- Pool admission is bounded FIFO first-idle scheduling. `pin()` holds a real
  epoch-scoped lease and returns an invalidatable proxy.
- Push buffering is epoch-scoped, bounded by frame and byte limits, drops the
  oldest data under pressure, and reports a sticky explicit gap.
- Heartbeat is an Actor timer. It has no extra thread and is deferred while the
  Actor mailbox or the weakly referenced pool Broker reports business pressure.
- Normal close, reopen, fatal failure, failed close and finalization preserve
  runtime identity rules and fail closed. Public facades are not retained by
  Actor targets or completion callbacks.

## Public Compatibility

Preserved:

- `TdxClient`, `TdxClient.from_hosts`, `SocketTransport`,
  `PooledSocketTransport`, context managers and existing business APIs.
- Existing 7709 commands, payloads, models, automatic first-request handshake,
  push polling/draining, `pool_size` and `heartbeat_interval`.

Added optional parameters, appended with defaults:

| Parameter | Default |
| --- | ---: |
| `max_pending_requests` | `256` |
| `push_queue_size` | `1024` |
| `push_queue_bytes` | `8 * 1024 * 1024` |

Added exceptions: `PoolBusyError`, `PushOverflowError`, and
`TransportCloseTimeoutError`.

Intentional behavior changes:

- For numeric IPs and cached endpoints, `timeout` is one end-to-end deadline
  across admission, connect, handshake, send, response and at most one retry.
- `pin()` now exclusively leases one slot instead of selecting a slot once.
- Push overload is bounded and reports a gap instead of growing without limit.
- Concurrent close cancels admitted unfinished requests and waits for owned
  resources, subject to the one-second fail-closed hard limit.

## Local Verification

Environment: Windows 11 `10.0.26200` AMD64, Intel i5-13400F, CPython 3.12.6.

| Time (+08:00) | Command | Result |
| --- | --- | --- |
| 2026-07-14 12:54 | `python -m pytest -q` at base | 102 passed |
| 2026-07-14 13:07 | frame/protocol targeted matrix | 53 passed |
| 2026-07-14 13:28 | Actor/frame/protocol targeted matrix | 72 passed |
| 2026-07-14 14:03 | pool/socket/client/resource matrix | 60 passed |
| 2026-07-14 14:25 | lifecycle/finalizer matrices | 23 and 47 passed |
| 2026-07-14 18:48 | `python -m pytest -q tests/test_transport_pool.py tests/test_transport_actor.py tests/test_socket_transport.py tests/test_transport_stress.py` | 40 passed in 31.66s |
| 2026-07-14 18:52 | `python -m pytest -q` | 183 passed in 24.98s |
| 2026-07-14 18:52 | `python -m build` | `eltdx-1.0.2` sdist and wheel built |
| 2026-07-14 18:52 | `python -m mkdocs build --strict` | PASS |
| 2026-07-14 19:09 | reconnect/socket/Actor targeted matrix | 24 passed in 0.71s |
| 2026-07-14 19:09 | `python -m pytest -q` | 183 passed in 25.22s |
| 2026-07-14 19:09 | `python -m build`; `python -m mkdocs build --strict` | sdist/wheel and strict docs PASS |

The final suite reported no skipped or xfailed tests. A source audit found no
`pytest.mark.skip` or `pytest.mark.xfail` in `tests/`. The added-code audit found
no debug breakpoint. Documentation examples and the two benchmark CLI JSON
writers are the only added `print()` calls.

The fresh final run initially found a single heartbeat throughput measurement
of 0.828189. It was not hidden by a rerun: five diagnostic repeats ranged from
0.970430 to 1.163034, proving that one sequential timing pair could not measure
a 1% gate. A warmed three-trial alternating-order prototype still found 11-17
heartbeats per 5,000 business requests and a 0.981280 ratio, exposing the real
missing Broker-pressure signal. The weak Broker guard and warmed multi-trial
benchmark fixed both issues. The final full suite and heavy benchmark below are
post-fix evidence.

The first FINAL attempt, `c403681`, kept the verified implementation unchanged
but its Ubuntu 3.11 CI run [29327752577](https://github.com/electkismet/eltdx/actions/runs/29327752577)
found a second deterministic-test issue: the reconnect test's second scripted
server returned immediately after sending a successful response. `execute()`
completed correctly, but the Actor could process the resulting EOF and clear
its live `connected_host` before the assertion. The harness now holds that
connection with an Event until the assertion finishes; runtime host semantics
remain unchanged and no sleep or timeout inflation is used.

## Cross-Platform CI

A09 verified implementation SHA: `b30c6b9ce885e29666f0e90a1565a195cf50074a`.

| Platform | Python | Job | Result |
| --- | --- | --- | --- |
| Ubuntu | 3.10 | `test (3.10)` | SUCCESS |
| Ubuntu | 3.11 | `test (3.11)` | SUCCESS |
| Ubuntu | 3.12 | `test (3.12)` | SUCCESS |
| Ubuntu | 3.13 | `test (3.13)` | SUCCESS, package build included |
| Windows | 3.11 | `windows-actor (3.11)` | SUCCESS |
| Windows | 3.13 | `windows-actor (3.13)` | SUCCESS |
| Ubuntu Pages | configured docs Python | `Build documentation site` | SUCCESS |

The package-build step is intentionally conditional on Python 3.13 and is
therefore shown as skipped in the 3.10-3.12 jobs; no test is skipped. Pages
deployment is intentionally skipped for a draft pull request, while the strict
site build and artifact upload succeed. GitHub emitted only the upstream
Node.js 20 action-deprecation warning.

## Stress And Resource Evidence

Exact final-code command:

```powershell
python scripts/stress_actor_transport.py --generations 10000 --requests 100000 --pool-size 4 --concurrency 100 --close-samples 100 --heartbeat-requests 10000 --idle-seconds 0.5 --output artifacts/actor_stress_final_v3.json
```

Artifact SHA256: `4C7B593BD38EA13696599D5DB34CD895DB91D9D704F338DF08C42021F56448F9`.
Workload SHA256: `C11B03DC9ABE8D842B4AAEC0B03396323B5BC29795D7B48F95C4A74731BA87F3`.

| Workload | Result |
| --- | --- |
| 10,000 TCP generations | 19.263s, 519.132 req/s, one Actor identity, 10,000 accepts, 0 stale events |
| 100,000 mixed requests | 54.025s, 1,850.988 req/s, server maximum active exactly 4 |
| Mixed reconnect/push | 103 accepts, 1,030 push frames, 6 bounded drops, one explicit gap, 0 stale events |
| Heartbeat | three 10,000-request trials per condition, median ratio 1.018972, max 4 heartbeats/trial (0.04%) |
| Idle CPU | 0.506659s wall, 0.000000s process CPU, ratio 0 |

| Resource/ownership check | Before | After | Gate |
| --- | ---: | ---: | --- |
| Generation stress Windows handles | 190 | 206 | delta 16, allowed <= 24 |
| Generation stress Actor threads | 0 | 0 | exact zero |
| Mixed stress Windows handles | 204 | 205 | delta 1, allowed <= 24 |
| Mixed stress Actor threads | active pool | 0 | exact zero |
| Mixed Broker waiters | bounded during load | 0 | exact zero after work |
| Mixed Broker leases | 4 active maximum | 0 | exact zero after work |
| Cross-generation response hits | 0 | 0 | exact zero |

Deterministic lifecycle tests additionally prove that close ends TCP sockets,
selector, both wakeup sockets, active/pending tickets, waiters, leases and
blocking push pollers. Close timeout tests retain the old runtime in
`FAILED_CLOSING`/`FAILED_CLOSED` and prohibit a second Actor.

Close latency over 100 samples per condition:

| Condition | p50 ms | p95 ms | p99 ms | Limit |
| --- | ---: | ---: | ---: | ---: |
| Idle | 1.5653 | 1.9380 | 2.0864 | 100 ms |
| Loaded | 1.1216 | 1.6883 | 1.9982 | 250 ms |

## Capacity Evidence

| Structure | Configured hard limit | Observed/verified evidence |
| --- | ---: | --- |
| Actor business mailbox | 1 | deterministic producer tests never admit a second active wire request |
| Pool admission | default 256 | limit-1 test observes one waiter and immediate `PoolBusyError` for the next; final state 0 |
| Normal active leases | `pool_size` | final mixed stress maximum 4, final state 0 |
| Push frames | default 1,024 | 1,030-frame stress fills 1,024 and drops 6 oldest with explicit gap |
| Push bytes | default 8,388,608 | same stress reaches 18,432 bytes; limit-36 test never exceeds 36 |
| Incremental RX buffer | 65,551 bytes | configured-32 garbage test reports `max_buffer_observed <= 32` |
| Resynchronization discard | 65,536 bytes | configured-1,000 test accepts exactly 1,000 then bounded error path is covered |
| Decoded-frame queue | 1,024 frames | 201-frame push-flood regression is consumed across 64-frame fairness slices without loss |
| Frame/decompressed payload | 65,535 bytes | declared/compressed/output/trailing-data boundary matrix passes |

## Performance Evidence

The acceptance workload uses the unchanged A01 benchmark script, SHA256
`27F80ADE31216BC5EB4879B26EA013FDD1C70DB8C828C26679B3E65458523960`,
with a fixed 5ms loopback response delay on the same Windows host.

Old artifact SHA256:
`559ED78556435A8161E99D6EC7EB131E190150FDBDFDF5E0D80864CCDFFA3FCE`.
Final-code artifact SHA256:
`6909B7EDD5E30209F2DEEAC8187DA671C69411836CC20849ED9EAC41D36D840C`.

The final artifact invokes the A01 `run_case` function exactly as follows:

```python
run_case(1, 1, 10_000, 0.005)
run_case(4, 100, 100_000, 0.005)
```

| Case | Old req/s | New req/s | Ratio | Required |
| --- | ---: | ---: | ---: | ---: |
| pool 1, concurrency 1, 10,000 | 170.250 | 167.618 | 98.454% | >= 95% |
| pool 4, concurrency 100, 100,000 | 682.586 | 658.319 | 96.445% | >= 95% |

Server maximum business concurrency is exactly 1 and 4 respectively. In the
sequential acceptance case, p50/p99 increased by 0.1017/0.1421ms, both below
the 0.2ms allowance. The retained 1ms diagnostic artifact has SHA256
`DDCA90A7ABDA152E53D3D4C9F97447F10DD8E67E60C8AAE6A6C6CE34E06D6DCD`
and observed scheduling-only p50/p99 increments of about 0.14/0.13ms. It is
not used for throughput acceptance because Windows timer granularity makes a
1ms sleeping-server throughput ratio unstable.

Additional deterministic performance properties pass: a 500ms slow slot does
not cause head-of-line scheduling behind it, and a caller parser blocked for
50ms does not retain the released wire slot.

## Review And Gate Audit

- Base-to-A09 diff: 31 files, 6,601 insertions, 400 deletions. Every file was
  reviewed against the file-level scope in the plan.
- Old reader thread, heartbeat thread, shared socket-owner and round-robin
  scheduler paths are absent or unreachable from the new transport.
- The Actor source contains no `create_connection`, `sendall`, `read_exact`,
  blocking `getaddrinfo`, or blocking queue use. Its only `send`/`recv` on the
  wakeup socketpair and TCP generation are non-blocking.
- No skip/xfail markers, debug breakpoint, unrelated source edit, unreviewed
  worktree diff, or task-owned background process remains.
- Documentation covers the state machine, end-to-end timeout, initial custom
  hostname DNS preflight exception, capacity controls, pin semantics, push gap,
  diagnostics, close and failed-closed behavior.
- Local branch, remote branch and draft PR were synchronized at A09. FINAL
  synchronization and exact-head CI are mandatory post-commit checks.

## Remaining Limits And Risks

- Standard-library DNS for a previously unseen custom hostname is a documented
  caller-side preflight and cannot be cancelled reliably. It holds no Actor,
  lease or TCP resource; the epoch is checked after resolution. Numeric default
  hosts and cached endpoints have the full end-to-end deadline.
- Finalizers are best-effort non-blocking stop+wakeup safety nets. Deterministic
  resource release still requires `close()` or a context manager.
- Performance evidence is local loopback on the specified Windows host. The
  deterministic scheduler/resource invariants and supported Python versions are
  additionally covered by CI, but real exchange latency is outside this task.
- GitHub Actions warns that `actions/checkout@v4` and `setup-python@v5` target
  deprecated Node.js 20. GitHub currently forces Node.js 24 and all jobs pass;
  future workflow dependency upgrades are maintenance, not a transport defect.

The PR remains intentionally unmerged. No tag, release, main-branch update, or
PyPI publication was performed or authorized.

## Recovery Rule

Future work must use this manifest, the checkpoint commits, the FINAL trailer,
the draft PR and the linked CI runs as authoritative evidence. Do not recreate
the deleted temporary ledger or restart A00. First resolve `SELF`, verify the
branch/PR head and CI for that exact SHA, then inspect any later commits or
worktree changes before acting.
