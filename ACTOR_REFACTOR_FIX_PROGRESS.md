# Actor Transport F10 Fix Progress

## Recovery Identity

- Objective: close F10 Push/Guard races, heartbeat measurement boundary, and permanent evidence.
- Branch: `actor-transport-refactor`
- Starting local/remote/PR HEAD: `5925c02f76a7a905d714900b1e16a907ada2f73f`
- Current production source: `45e67038bab23096c707b0f6b57fa9ecd19e3e4f`
- Draft PR: `https://github.com/electkismet/eltdx/pull/12` (OPEN, Draft, unmerged)
- User dirty paths at start: none.
- Non-goals: no Actor redesign, no 7615/F10 business changes, no protocol/API/dependency/workflow/Pages structure changes, no main/tag/release/package publication.

## Stage Status

| Stage | Status | Result / next action |
| --- | --- | --- |
| F10-SPEC | completed | Revision 1.4 frozen at `242e7e5`; Plan blob SHA256 `63D9BF2CA2568D1CC71DEDDAA5A16B252676F8625035B9493B93149C5D51562D`. |
| F10-RED | completed | Three behavior-only tests fail deterministically on old production code for the required reasons. |
| F10-FIX | in_progress | Apply minimal Push and Guard fixes. |
| F10-HEARTBEAT | pending | Implement phase-scoped business-window heartbeat measurement. |
| F10-TEST | pending | 20 processes, targeted matrix, clean full pytest, stress, performance, package, MkDocs. |
| F10-EVIDENCE | pending | Update permanent manifest and checkpoint identity; complete independent reviews. |
| FINAL | pending | Delete this ledger, push exact HEAD, wait for CI/Pages, post PR delivery comment. |

## Current State

- Current HEAD before this checkpoint: `242e7e5dd19eebd47d5ed7d91e0e295b8f642d64`.
- Last completed: F10-RED deterministic reproduction on unchanged production source `45e6703`.
- Current phase: F10-FIX.
- Next exact action: commit and push behavior-only RED tests, then change only `PushBuffer.publish_close()` and `PoolRuntimeGuard.failure()` as specified by revision 1.4.
- Pending push: F10-RED commit to be created and pushed.

## Verification Log

| Time (Asia/Shanghai) | Source | Command | Result |
| --- | --- | --- | --- |
| 2026-07-19 | `5925c02` | `git status --short --branch`; `git rev-parse HEAD`; `git ls-remote origin refs/heads/actor-transport-refactor`; `gh pr view 12 --json ...` | Clean; local/remote/PR all `5925c02`; PR OPEN/Draft/unmerged. |
| 2026-07-19 | working tree | `git diff --check` | PASS; revision 1.4 and ledger have no whitespace errors. |
| 2026-07-19 | `242e7e5` with test-only working tree | `python -m pytest -q tests/test_transport_retirement_regressions.py::test_standalone_owner_normal_close_cannot_overwrite_actor_fatal_after_stale_read tests/test_transport_retirement_regressions.py::test_guard_failure_non_none_fast_path_rechecks_publication_snapshot_identity tests/test_transport_stress.py::test_heartbeat_after_final_business_response_is_outside_business_window` | Expected RED: 3 failed in 0.83s. Push fatal became `None` after owner resumed; Guard returned the old epoch exception; heartbeat phase-window API was absent. |

## Known Failures And Risks

- Historical clean full suite at `5925c02`: `1 failed, 645 passed`; heartbeat total counter included one legal post-business/pre-disable heartbeat.
- P1-A and P1-B RED proof is committed in the next checkpoint; GREEN proof is pending.
- Plan revision 1.4 must not change after F10-SPEC is committed; Result must use the committed blob SHA256.
- No retry has been used to hide a failure. Network retry count: 0.
