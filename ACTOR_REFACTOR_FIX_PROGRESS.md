# Actor Transport Refactor Fix Progress

## Reopened Review Identity

- Reopened: 2026-07-17 Asia/Shanghai.
- Branch: `actor-transport-refactor`.
- Reopened parent HEAD: `7f8e120dbddf37197f7718f8712589184cc20df8`.
- Draft PR: https://github.com/electkismet/eltdx/pull/12 (OPEN, draft, unmerged).
- Status: second checkpoint implementation and local verification complete;
  FINAL evidence is still in progress.
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

- Create and push the non-interactive performance-correction checkpoint commit.
- Confirm local, remote branch, and PR HEAD identities and inspect exact-head CI/Pages.
- Run the fresh exact-checkpoint performance campaign against baseline
  `9a60e769`, plus 10,000 generations, 100,000 unique requests, two-server
  failover stress, heartbeat, Windows resource warmup/repetition, and thread/
  socket/selector/wakeup/ticket/waiter/lease cleanup evidence.
- Run package build, MkDocs strict/Pages build, and the required Windows and
  Ubuntu/Python matrix (local where applicable and exact-head CI).
- Re-run final independent adversarial reviews after all evidence is frozen.
- Replace the overturned conclusion in `ACTOR_REFACTOR_RESULT.md`, move all
  evidence there, then delete this temporary file in the FINAL commit.
- Wait for exact FINAL HEAD CI and Pages success. Do not merge the PR.

Do not delete this file until all replacement FINAL evidence is recorded in
`ACTOR_REFACTOR_RESULT.md` and exact FINAL HEAD CI/Pages have succeeded.
