# Actor Transport F10 R5 Fix Progress

## Recovery Identity

- Objective: close the exact-final out-of-order heartbeat phase fence found after F10 R4.
- Branch: `actor-transport-refactor`.
- Reopened delivery HEAD: `64bbea9bd1965426e30e24f4f10e99d1144a831b`.
- Production source before R5: `6abbaf5273aace450972f6e99fe44a8f05307812`.
- Draft PR: `https://github.com/electkismet/eltdx/pull/12` (OPEN, Draft, unmerged).
- User dirty paths at reopen: none.
- Plan revision 1.4 remains frozen at `242e7e5`; no architecture or scope expansion.

## Stage Status

| Stage | Status | Result / next action |
| --- | --- | --- |
| R5-REVIEW | completed | Exact-final review found an out-of-order phase response could bypass the wire fence. |
| R5-RED | completed | Real `_serve()` interleaving failed because the out-of-order closer sent while the phase wire lock was held. |
| R5-FIX | completed | Every response in the active phase now shares the phase wire lock for send and finish. |
| R5-TEST | completed | 20 processes, targeted/full, clean stress, package and MkDocs complete; performance diagnostic retained failed without retry. |
| R5-EVIDENCE | in progress | Permanent R5 evidence updated; exact-source independent rereviews remain. |
| FINAL | pending | Delete this ledger, push exact HEAD, wait for CI/Pages, update the PR delivery comment. |

## Verification Log

| Time (Asia/Shanghai) | Source | Command | Result |
| --- | --- | --- | --- |
| 2026-07-19 | `64bbea9` | exact-final Push/Guard review and four deterministic nodes | Push/Guard CLEAN; `4 passed in 0.29s`. |
| 2026-07-19 | `64bbea9` | exact-final heartbeat phase review | Reopened: request sequence does not determine response completion order, so a non-final-sequence response can be the actual phase closer without holding the wire lock. |
| 2026-07-19 | `64bbea9` plus test-only working tree | `python -m pytest -q tests/test_transport_stress.py::test_out_of_order_final_heartbeat_response_is_wire_fenced` | Expected RED: `1 failed in 0.54s`; the first-sequence response sent while the main thread held the phase wire lock. |
| 2026-07-19 | R5 working tree | three deterministic heartbeat boundary nodes | GREEN: `3 passed in 0.38s` after fencing every phase response. |
| 2026-07-19 | `f12701f` | three R5 heartbeat boundary nodes in 20 independent pytest processes | GREEN: `60/60`; 20 processes passed, no failed process. |
| 2026-07-19 | `f12701f` | Push/Guard/heartbeat/retirement/lifecycle/stress targeted matrix | GREEN: `262 passed in 239.23s (0:03:59)`. |
| 2026-07-19 | `f12701f` | `python -m pytest -q` from zero | GREEN: `651 passed in 246.78s (0:04:06)`; no retry or result splicing. |
| 2026-07-19 | `f12701f` plus progress-only working tree | first R5 heavy stress diagnostic | Behavior gates passed, but artifact self-reported `worktree_dirty=true`; excluded from formal evidence and must be rerun after this checkpoint. |
| 2026-07-19 | clean exact `1dd1dcd` | 10k generations / 100k requests stress/resource | PASS: all ownership counters 0, max active 4, heartbeat windows `0/0`, throughput ratio `0.998671`, resources `192 x8`, SHA256 `9B7CF774E8E488715EF6C0DD5397EDB5E854B1B78EA0399783D985386C59DF39`. |
| 2026-07-19 | clean exact `1dd1dcd` | declared 8-cell performance campaign `f10-r5-1dd1dcd` | Integrity FAIL: first three attempt-1 cells completed; index 3 baseline exited `RPC_NT_INTERNAL_ERROR (0xC0020043)`; stopped and not retried. Existing canonical campaign remains authoritative because runtime/producer/verifier are unchanged. |
| 2026-07-19 | clean exact `1dd1dcd` | `python -m build`, `python -m twine check`, `python -m mkdocs build --strict` | PASS: wheel `F899A9...A307`, sdist `F1C162...5AC`, MkDocs 126 files / 5,685,815 bytes. |
| 2026-07-19 | exact `fd2070c` independent code review | Production measurement fix CLEAN; one P3 found because the R5 RED used a 50ms negative scheduling assertion. Replaced it with a positive tracking-lock/send progress latch before rereview. |
| 2026-07-19 | strengthened R5 test working tree | three boundary nodes and 20 independent processes | GREEN: `3 passed in 0.38s`, then `60/60`; no failed process. |
| 2026-07-19 | exact `ff3b1e0` independent heartbeat review | Production protocol CLEAN; P2 test gap found because real `_serve()` did not yet prove send+finish atomicity or failed-send cleanup. Added positive lock-attempt barriers around a blocked actual closer and a real raising-send test. |
| 2026-07-19 | final R5 test working tree | four boundary nodes, 20 processes and stress file | GREEN: `4 passed in 0.39s`, `80/80`, and `30 passed in 229.87s`; no failed process. |
| 2026-07-19 | final R5 test working tree | targeted matrix and complete pytest from zero | GREEN: `263 passed in 239.78s`; full `652 passed in 246.80s`; no retry or result splicing. |

## Current State

- Current phase: R5-EVIDENCE.
- Next exact action: commit the P2 coverage correction, rebuild package evidence on the clean exact SHA, then complete independent rereviews.
- Pending push: R5 evidence checkpoint and final delivery.
