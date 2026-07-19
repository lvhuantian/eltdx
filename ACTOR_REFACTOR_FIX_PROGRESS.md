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
| R5-FIX | pending | Fence every response belonging to the active heartbeat business phase. |
| R5-TEST | pending | Run independent-process, targeted, full, stress, package and MkDocs gates as required. |
| R5-EVIDENCE | pending | Update permanent evidence and independent review conclusions. |
| FINAL | pending | Delete this ledger, push exact HEAD, wait for CI/Pages, update the PR delivery comment. |

## Verification Log

| Time (Asia/Shanghai) | Source | Command | Result |
| --- | --- | --- | --- |
| 2026-07-19 | `64bbea9` | exact-final Push/Guard review and four deterministic nodes | Push/Guard CLEAN; `4 passed in 0.29s`. |
| 2026-07-19 | `64bbea9` | exact-final heartbeat phase review | Reopened: request sequence does not determine response completion order, so a non-final-sequence response can be the actual phase closer without holding the wire lock. |
| 2026-07-19 | `64bbea9` plus test-only working tree | `python -m pytest -q tests/test_transport_stress.py::test_out_of_order_final_heartbeat_response_is_wire_fenced` | Expected RED: `1 failed in 0.54s`; the first-sequence response sent while the main thread held the phase wire lock. |

## Current State

- Current phase: R5-FIX.
- Next exact action: commit the behavior-only RED checkpoint, then fence every response belonging to the active phase.
- Pending push: R5 RED checkpoint and all later R5 checkpoints.
