# Actor Epoch-Retirement Fatal Reason Correction Progress

## Recovery identity

- Objective: make one epoch-scoped fatal reason monotonic and sticky across Guard, Push, `finish_epoch`, pinned push readers, and repeated owner reads; complete deferred Push drain without blocking an Actor or changing graceful buffered-close behavior.
- Branch: `actor-transport-refactor`
- Refactor base: `71089c0a2867a75dc79aa2c340213f4e3845b6e3`
- Cycle baseline / local HEAD at start: `45d8bc80f65eb57ee4ff5fab9a420d80aa705c6a`
- Remote branch HEAD at start: `45d8bc80f65eb57ee4ff5fab9a420d80aa705c6a`
- Draft PR #12 HEAD at start: `45d8bc80f65eb57ee4ff5fab9a420d80aa705c6a`
- Started: `2026-07-18T18:50:44+08:00`, Windows 10 build 26200, CPython 3.12.6.
- User-owned dirty paths at start: none (`git status --porcelain=v2` empty).
- Repository `AGENTS.md`: not present under `C:\Users\ax\Desktop\eltdx`; the task-supplied AGENTS instructions are authoritative.

## Scope and non-goals

- In scope: `actor.py`, `pool.py`, `push.py` fatal/retirement publication; Push owner-side lazy drain; deterministic regressions; transport/stress/performance/package/docs evidence; exact-head CI/Pages and final independent review.
- Non-goals: `main`, F10/7615, market protocol APIs, runtime dependencies, Pages structure, release, tag, merge, or force-push.
- Actor fatal publication may write only its pre-registered single-writer cell plus runtime signals; it must not wait for Pool, Broker, Push, Proxy, resolver, or sibling Actor application locks.

## Checkpoints

| Checkpoint | Status | Evidence |
| --- | --- | --- |
| FATAL-R00 baseline and RED | complete | Baseline 30 passed; unchanged production with final new regressions: 9 failed, 31 passed in 0.85s |
| FATAL-R01 production correction | complete | production `a987c16`, resolver/lazy drain and docs pushed |
| FATAL-R02 focused and 20-process verification | complete | 20 independent processes, 40 passed each; log SHA256 recorded below |
| FATAL-R03 full correctness/stress/performance/build/docs | pending | not run |
| FINAL | pending | permanent manifest, ledger deletion, exact-head CI/Pages, three CLEAN reviews |

## Current state

- Current unique `in_progress`: run the full pytest suite and then freeze stress/performance/build/docs evidence.
- Last completed: production checkpoint `a987c16` is pushed; 20 independent retirement processes all passed 40 tests.
- Next exact action: run `python -m pytest -q`, inspect skip/xfail counts, then run the exact 10k/100k stress command from a clean source and save its SHA256.
- Modified task paths: `ACTOR_REFACTOR_FIX_PROGRESS.md` only; generated 20-process log is outside the worktree.
- Push status: remote branch is `a987c16`; this evidence update is local and will be committed as `F07E`.
- PR status: OPEN, Draft, unmerged.

## Commands and results

| Time | Command | Result |
| --- | --- | --- |
| 2026-07-18 18:46 +08:00 | `python -m pytest -q tests\\test_transport_retirement_regressions.py` | PASS, 30 passed in 0.36s on unchanged baseline |
| 2026-07-18 18:46 +08:00 | `git diff --check` | PASS |
| 2026-07-18 18:52 +08:00 | `python -m pytest -q tests\\test_transport_retirement_regressions.py` | EXPECTED RED, initial 8 failed / 32 passed in 0.85s |
| 2026-07-18 18:55 +08:00 | same command after strict old/new epoch identity assertions | EXPECTED RED, final 9 failed / 31 passed in 0.85s |
| 2026-07-18 19:03 +08:00 | `python -m pytest -q tests\\test_push_buffer.py tests\\test_transport_retirement_regressions.py` | PASS, 48 passed in 0.41s |
| 2026-07-18 19:05 +08:00 | full transport matrix (retirement, push, socket, actor, pool, lifecycle, failover) | PASS, 428 passed in 16.17s |
| 2026-07-18 19:05 +08:00 | `python -m compileall -q src tests` | PASS |
| 2026-07-18 19:09 +08:00 | 20 independent `python -m pytest -q tests\\test_transport_retirement_regressions.py --tb=short` processes | PASS, 20 x 40 passed; log `C:\\Users\\ax\\Desktop\\eltdx\\artifacts\\actor-retirement-20proc-a987c16.log`, SHA256 `D8AE349AE3D71A9A3EA5481FECB0D8C72F2B71C0C831560AE9490A010AEB3282` |

## Open risks and failures

- RED: retire set before Push fanout returned no error instead of the exact fatal.
- RED: with failure-cell contention Guard changed from the first observed fatal object to a later-scanned object.
- RED: controlled double publication left Guard on the second error while Push was overwritten by the resumed first error.
- RED: after failed `abandon()` try-lock, `pending_count`, `snapshot`, `poll`, and `drain` all left one frame/17 bytes and `closed=False`.
- RED: pinned `poll_push()` / `drain_pushes()` replaced the epoch fatal object with a new `ConnectionClosedError`.
- RED coverage also requires the old epoch Push to retain its delayed fatal object and a stale old `finish_epoch` call never to return the new epoch fatal.
- Earlier implementation attempt caused 5 lifecycle cleanup failures by draining failed `close(error)` publications; corrected by limiting lazy drain to permanent retirement or `abandon()` try-lock failure. Re-run: 428 transport cases passed.
- Existing `ACTOR_REFACTOR_RESULT.md` still has obsolete exact-source CI/Pages `PENDING` language and production/evidence identities from the previous correction; update only after the new evidence is frozen.
- Unrelated running Python processes at baseline are external to this task (`uvicorn axquant` and a separate `python -` process); do not terminate or alter them.
