# Actor Transport Refactor Fix Progress

## Reopened Review Identity

- Reopened: 2026-07-17 Asia/Shanghai.
- Branch: `actor-transport-refactor`.
- Reopened parent HEAD: `7f8e120dbddf37197f7718f8712589184cc20df8`.
- Draft PR: https://github.com/electkismet/eltdx/pull/12 (OPEN, draft, unmerged).
- Status: exact `48b32d6` evidence is superseded for FINAL after a later
  deterministic stale-timeout publication blocker; the per-call correction
  and new evidence are in progress.
- The old `9a60e76` completion conclusion remains overturned. The green checks on
  `7f8e120` are historical and cannot validate the upcoming checkpoint.

## Closed Reopened Findings

| Finding | Resolution and deterministic evidence |
| --- | --- |
| Stale `IdentityGate` publication can overwrite the current owner release | Each owner now has an identity-bound publication cell; stale publication regression passes. |
| Deferred lease completion can strand or incorrectly admit Broker waiters | Completion wakes registered waiters; non-head waiters recheck FIFO state. The capacity and non-head FIFO regressions pass. |
| Pin completion can strand waiters or lose a wake during Event clear | Pin-local publication wakes waiters and rechecks after clear. The controlled clear-window regression passed 50/50 in independent review and fails with the stale precheck restored. |
| Push gap/drop state can lose concurrent drops | Per-Actor monotonic drop counters replace the shared gap flag; controlled drop regressions pass. |
| Push terminal publication can expose close before payload and block Actor cleanup | Payload precedes terminal publication and Actor cleanup no longer acquires the Push condition. |
| Old pool epoch failure publication can mask the current epoch | Each epoch has an identity-bound failure cell and try-lock; the paused old-epoch publisher regression passes and old behavior fails. |
| Startup/fatal settlement can lose the original absolute deadline | Startup registration and fatal settlement reuse the original deadline; delayed-start regression passes. |
| Internal Lease/Pin settlement errors can be swallowed or replace the wire error | Completion errors reach the caller; wire/timeout errors remain primary with cleanup as exact `__cause__`. |
| Unexpected fatal cleanup can be erased by a successful retry | An unreported unexpected error is surfaced exactly once by public close, including the real Pool/ActorFatalHandle path; the second close reaches `FAILED_CLOSED` with resources cleared. |
| Layered unexpected -> timeout -> success can orphan the first error | Public close reports the first unexpected error independently of the current retry error; all primary/deferred/fatal references clear after reporting. |
| Pre-thread structured fatal timeout cannot clear after retry | `_fail_actor_startup` records structured settlement failures in the same identity-based ledger; a successful close retry clears primary/deferred/fatal references. |
| Legacy owner-cleanup callback leaves terminal traceback references | Legacy ignored callback failures no longer populate fatal or unreported cleanup slots; terminal runtime assertions pass. |

## Current Local Evidence

- New Lease/Pin/error-chain/startup/epoch focused selection: 20 independent
  pytest processes, each `6 passed`.
- New fatal settlement sequence selection: 20 independent pytest processes,
  each `6 passed`.
- Lifecycle regression file after the final startup correction:
  `92 passed in 7.52s`.
- Push/Socket/Actor/Pool/lifecycle/failover nine-file matrix after the final
  correction: `376 passed in 16.48s`.
- Frozen-worktree complete suite, correct entry point:
  `python -m pytest -q` -> `586 passed in 259.62s` on Windows CPython 3.12.6.
- `git diff --check`: no content errors; only expected LF-to-CRLF worktree warnings.
- Actor ownership/nonblocking independent review: CLEAN on the latest tree.
- Pool/lease/pin/close lifecycle independent review: CLEAN on the latest tree.
- Test evidence independent review: CLEAN after inspecting the latest
  regressions and the frozen-worktree full result.

## Performance-Gate Correction After `8b685420`

The first quick A/B attempt is invalid and retained only as an audit failure:
the baseline ran workload hash `4ddd761f...` while current ran `b09ab713...`.
Those files were renamed with an `invalid-` prefix and are not used for ratios.

A corrected same-workload A/B used the current frozen benchmark script
(`b09ab713...`) with only `ELTDX_SOURCE_ROOT` changed. Both roots were clean and
exact (`9a60e769` baseline, `8b685420` current), all 90,000 responses were
unique, and all error/cross-request/cross-generation counters were zero. It
found a real admission performance blocker:

- concurrency 1 throughput ratio: about `0.990` for pool size 1;
- concurrency 100 ratios: `0.611`, `0.537`, and `0.365` for pool sizes 1, 2,
  and 4;
- root cause: every successful request published the same Lease completion
  more than once, and every publication set every queued waiter Event.

The retained raw diagnostic files are
`pre-fix-performance-fail-baseline-a-9a60e769.json` and
`pre-fix-performance-fail-current-a-8b685420.json`. They motivate the fix but
are not FINAL performance evidence.

The current dirty checkpoint adds a durable lease-release pulse, condition-
owned clear/reclaim/assign/recheck, first-live FIFO baton handoff, cancelled/
expired waiter skipping, and successful Lease publication idempotency. Actor
publication remains lock-free with respect to Broker/Pool/Proxy ownership.

Current correction evidence:

- Six controlled pulse/live-head/idempotency regressions: 20 independent
  processes, each `6 passed`.
- Pool files: `90 passed in 1.99s`.
- Nine-file transport matrix: `382 passed in 16.09s`.
- Frozen complete suite: `592 passed in 271.35s`.
- Dirty-tree performance probe: pool 1/concurrency 1 `159.585 rps`; pool
  4/concurrency 100 `607.188 rps`; all 6,000 responses unique and both cross
  counters zero.
- Independent Actor nonblocking review: CLEAN.
- Independent Pool/FIFO review and its separate adversarial child review:
  CLEAN, including batch FIFO, duplicate/late completion, 16 concurrent
  publishers, and publish/close/abandon probes.

One earlier full-suite run (`585 passed, 1 failed`) is explicitly invalid: it
started while `_fail_actor_startup` was being edited and observed a transient
`settle=None` call that was removed before the frozen run. It is not evidence
for either correctness or a current failure.

## Remaining Before FINAL

- Freeze and push the per-call pin publication correction, then rerun exact
  full/stress/resource/build/CI evidence and final independent reviews.
- Replace the overturned conclusion in `ACTOR_REFACTOR_RESULT.md`, move all
  evidence there, then delete this temporary file in the FINAL commit.
- Wait for exact FINAL HEAD CI and Pages success. Do not merge the PR.

Do not delete this file until all replacement FINAL evidence is recorded in
`ACTOR_REFACTOR_RESULT.md` and exact FINAL HEAD CI/Pages have succeeded.

## Final-Review Reopen After `f5ad8a3`

The final Pool/lifecycle review found that
`PinnedTransportProxy._settle_published_terminal` cleared one shared Event
after settling an old call without rechecking `_published_terminal_call`. If
the old terminal assigned the next FIFO call and that call published before
the old settler cleared the Event, the new call's wake was erased.

The deterministic red selection covers a real old-terminal assignment plus
new publication before return, controlled publication before the actual
`Event.clear`, publication after the actual clear, and exact-old cleanup:

```text
2 failed, 2 passed, 79 deselected in 0.88s
```

Both failures are the expected lost-new-publication assertions on exact
`f5ad8a3`; the after-clear and exact-old controls pass. The current correction
clears the old Event, rereads the monotonic call identity, and restores both
the publication Event and waiter-snapshot wakes if a newer call appeared in
any clear window.

Correction verification on the frozen worktree:

- five focused publication/deadline tests: 20 independent pytest processes,
  each **5 passed**;
- complete Pool regression file: **83 passed in 1.94s**;
- complete lifecycle regression file: **92 passed in 7.78s**;
- Actor/Pool core selection: **156 passed in 4.14s**;
- complete suite: **596 passed in 273.14s**;
- independent Pool/lifecycle review: **CLEAN**, including two controlled
  concurrent-settler sequences and final capacity cleanup;
- `git diff --check`: PASS with only expected LF-to-CRLF worktree warnings.

The final evidence review also rejected the four schema-2 `actor-lock-l03p`
A/B JSON files as input to the frozen schema-4 performance verifier. Their
same-workload ratios and clean identities remain useful correction-regression
data, but they must be labeled supplemental. The retained formal schema-4
campaign against `71089c0` remains the historical architecture-performance
FAIL authorized by the user-selected ownership/FIFO/pool-size design; it is
not reclassified by the supplemental `9a60e769` comparison.

## Exact `48b32d6` Evidence Checkpoint

`48b32d6f248fd0e36a2e5d8199e21e5dc0215a61` (`Fix-Checkpoint: L03R`)
contains the pin publication identity fix and four new deterministic
regressions. Local, remote branch and Draft PR #12 head were exact; the PR
remained OPEN, draft, unmerged, base `main`.

- CI [run 29573695623](https://github.com/electkismet/eltdx/actions/runs/29573695623):
  SUCCESS. Ubuntu Python 3.10-3.13 each ran 595 passed/1 Windows-only skip;
  Windows 3.11/3.13 ran 596 passed; the Python 3.13 package build passed.
- Pages [run 29573695645](https://github.com/electkismet/eltdx/actions/runs/29573695645):
  strict build/upload SUCCESS; PR deployment skipped as expected.
- Frozen local complete suite: **596 passed in 273.14s**.
- Local `python -m build` and `twine check`: PASS for both artifacts.
  Wheel: 308,699 bytes, SHA256
  `7E3D3E967DC5298DF49A1DF092D2F2494AAEC6855395663F9943D0917A6DE72B`.
  Sdist: 366,467 bytes, SHA256
  `79BC290FCD62A44C1488629ACC588EE0644B9BE543447C833FC85E075B89989F`.
- Local MkDocs strict build: PASS in 2.81s.

Retained exact stress artifact:
`actor-lock-l03r-stress-48b32d6.json`, 736,906 bytes, SHA256
`D874E3346479C78592BE205C595EE2EEC7AD18AD8C19586F1BA781E24E8079E3`.
It records exact clean `48b32d6` on Windows 11 / Python 3.12.6:

- 10,000 generations retain one Runtime/Actor/thread identity, use two real
  loopback servers, return 10,000 unique values and leave every ownership,
  stale and cross counter at zero;
- 100,000 requests at pool 4/concurrency 100: 853.371 rps, server maximum
  active exactly 4, 36 real cross-endpoint retries, 100,000 unique values and
  all duplicate/missing/unexpected/cross counters zero;
- after close, Actor threads, TCP, selectors, wakeups, tickets, cancels,
  Broker waiters/pin waiters/leases and Push frames/bytes are all cleared or
  closed;
- idle close p50/p99 3.1005/4.1792ms; loaded 2.7476/4.1612ms;
- heartbeat ratio 1.002961, absolute impact 0.2961% <1%, 35,232 unique and
  cross counters zero;
- three warmups then eight measured Windows resource rounds are exactly
  `202,202,202,202,202,202,202,202`, with no monotonic growth; idle CPU is 0.

The exact correction supplemental A/B used fixed order
baseline/current/current/baseline, clean roots `9a60e769` and `48b32d6`, and
workload SHA256
`b09ab7130752ae0c562b63ba04d2b1bea42f1e168c060f13d6e86e9bba277b84`.
All 180,000 responses and completion rows reconcile; every error, duplicate,
missing, unexpected, cross-request and cross-generation count is zero.

| Artifact | SHA256 |
| --- | --- |
| `actor-lock-l03r-baseline-a-9a60e769.json` | `0575A797800E412C0095D4ED838C29B0FFC6132808CB5CCA811B0DD8E90399FA` |
| `actor-lock-l03r-current-a-48b32d6.json` | `A4551C3AC99AAA8F4AC1A9FB28E7297B0239882001829F15F8EBAA15089FB4BC` |
| `actor-lock-l03r-current-b-48b32d6.json` | `913F5CDFF61C16BEC21B9A57AD87CD49A4CE4FDFE573B4550A6C68941134E692` |
| `actor-lock-l03r-baseline-b-9a60e769.json` | `E12E64DDEC4FAC54EE76EA79067BE617B63A8B7557C5E4D7F2B4440628C61D62` |

Across nine cases, pooled throughput ratios are 0.969600-0.995321. Sequential
baseline/current is 157.217604/156.481948 rps; pool 4/concurrency 100 is
601.914089/583.615987 rps. The schema-2 files contain per-run quantiles rather
than raw latency arrays, so two-run p50/p99 values cannot be pooled. Required
sequential and pool 4/concurrency 100 pair diagnostics pass the latency
allowance, but pool 4/concurrency 10 pair B p99 is 21.1102/24.2162ms and exceeds
its 2.11102ms allowance by 0.99498ms. These files are supplemental correction-
regression evidence only and are not accepted by the frozen schema-4 verifier.

## Final-Review Reopen After `166ae61`

The final Actor review found a second deterministic pin publication blocker in
the exact `48b32d6` source carried by evidence checkpoint `166ae61`. A stale
call-1 settler can pass `_settle_published_terminal`'s first condition barrier,
then lose `_wire_terminal(1)`'s second condition acquisition after call 2 has
already become active and published. The timeout fallback unconditionally
republished call 1 over call 2; the next owner then cleared the stale Event and
left active call 2 without a terminal publication.

The two behavior-only regressions were red on an exact detached `166ae61`
worktree after applying only the tests:

```text
2 failed in 1.59s
assert proxy._active_call is None
E assert 2 is None
```

One test forces the old settler's second condition acquisition to expire after
call 2 publishes; the other directly replays call 1 after call 2 publishes.
Both therefore fail on the missing call-2 settlement rather than on a new
implementation field. The temporary red worktree was removed after the run.

The current correction replaces the shared latest-call/Event register with one
immutable `_PinActiveCall` snapshot containing the exact call ID and its own
Event. Active-call transition is one object-reference replacement. Actor and
timeout publishers only set the Event in their captured exact state; an old
state can neither replace nor clear a newer state. The Proxy retains only the
one active state, so publication remains bounded and the Actor still acquires
no Proxy/Broker/Pool lock.

Current dirty-tree evidence after the behavior-only assertion correction:

- eight success/timeout/direct-stale/before-during-after-clear/exact-old/
  deadline cases: 20 independent pytest processes, each **8 passed**;
- complete Pool regression file: **85 passed in 1.92s**;
- complete lifecycle regression file: **92 passed in 7.82s**;
- Actor/Pool core selection: **156 passed in 4.13s**;
- frozen complete suite: **598 passed in 268.96s**. Source SHA256
  `4AE60312C9BD979E9D1B4204398BDCDF761E1E1A09D857804061D73A76DD2C96`
  and test SHA256
  `A456103653C11A40104559792DA956929A1807A552091A0A860A67B84D776AAD`
  were identical before and after the run;
- independent Actor/nonblocking review: **CLEAN**;
- independent Pool/pin/lifecycle review: **CLEAN** after requesting behavior-
  only red assertions; the regressions and 20-process focused evidence were
  rerun after that correction.

The exact `48b32d6` CI, stress and supplemental A/B remain valid historical
checkpoint evidence but cannot validate the new source. Full, stress/resource,
build, CI and review evidence must be regenerated after the next clean commit.
