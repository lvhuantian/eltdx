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
| R5-TEST | in progress | Focused RED/REEN is green; independent-process and full gates remain. |
| R5-EVIDENCE | pending | Update permanent evidence and independent review conclusions. |
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

## Current State

- Current phase: R5-TEST.
- Next exact action: commit this test checkpoint, then rerun the 10k/100k stress campaign on its clean exact SHA.
- Pending push: R5 test/evidence checkpoints and final delivery.
