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
| F10-FIX | completed | Push normal close no longer writes `None`; Guard non-None fast returns require unchanged snapshot identity. |
| F10-HEARTBEAT | completed | Server-owned phase window closes on the final business response; post-response heartbeat remains total-only, including the sendall-to-close interleaving. |
| F10-TEST | in_progress | First validation complete; independent review found and fixed sendall-to-window-close heartbeat race; all validation must be rerun. |
| F10-EVIDENCE | pending | Update permanent manifest and checkpoint identity; complete independent reviews. |
| FINAL | pending | Delete this ledger, push exact HEAD, wait for CI/Pages, post PR delivery comment. |

## Current State

- Current HEAD before this checkpoint: `6c344d468dafe59a240edbb08001264e91f2aeb4`.
- Last completed: first F10 validation and independent review; P2 sendall-to-window-close heartbeat race fixed in working tree.
- Current phase: F10-TEST.
- Next exact action: commit F10 heartbeat race correction, then rerun 20 processes, targeted/full pytest and stress; retain the already completed immutable performance campaign without resampling.
- Pending push: F10-HEARTBEAT-R2 commit to be created and pushed.

## Verification Log

| Time (Asia/Shanghai) | Source | Command | Result |
| --- | --- | --- | --- |
| 2026-07-19 | `5925c02` | `git status --short --branch`; `git rev-parse HEAD`; `git ls-remote origin refs/heads/actor-transport-refactor`; `gh pr view 12 --json ...` | Clean; local/remote/PR all `5925c02`; PR OPEN/Draft/unmerged. |
| 2026-07-19 | working tree | `git diff --check` | PASS; revision 1.4 and ledger have no whitespace errors. |
| 2026-07-19 | `242e7e5` with test-only working tree | `python -m pytest -q tests/test_transport_retirement_regressions.py::test_standalone_owner_normal_close_cannot_overwrite_actor_fatal_after_stale_read tests/test_transport_retirement_regressions.py::test_guard_failure_non_none_fast_path_rechecks_publication_snapshot_identity tests/test_transport_stress.py::test_heartbeat_after_final_business_response_is_outside_business_window` | Expected RED: 3 failed in 0.83s. Push fatal became `None` after owner resumed; Guard returned the old epoch exception; heartbeat phase-window API was absent. |
| 2026-07-19 | F10-FIX working tree | `python -m pytest -q tests/test_push_buffer.py tests/test_transport_retirement_regressions.py` | GREEN: 55 passed in 0.56s. Includes both new Push/Guard race tests and existing fatal identity/retirement coverage. |
| 2026-07-19 | F10-HEARTBEAT working tree | `python -m pytest -q tests/test_transport_stress.py::test_heartbeat_after_final_business_response_is_outside_business_window` | GREEN: 1 passed in 0.40s; post-response heartbeat total is 1 and business-window count is 0. |
| 2026-07-19 | F10-HEARTBEAT working tree | `python -m pytest -q tests/test_transport_stress.py -k "heartbeat and not idle_actor_blocks"` | GREEN: 22 passed, 5 deselected in 18.41s. |
| 2026-07-19 | F10-HEARTBEAT working tree | three new F10 nodes together | GREEN: 3 passed in 0.25s. |
| 2026-07-19 | `2aea276` | three new F10 nodes, 20 independent pytest processes | GREEN: 60/60 cases; log `artifacts/actor-f10-20proc-2aea276.log`, SHA256 `DC3BCECE67A8802E67CB0417C48659F6BDCD556CFE422564B3F32C23646826AC`. |
| 2026-07-19 | `2aea276` | `python -m pytest -q tests/test_push_buffer.py tests/test_transport_retirement_regressions.py tests/test_transport_lifecycle_regressions.py tests/test_transport_pool_regressions.py tests/test_transport_stress.py` | GREEN: 260 passed in 253.39s. |
| 2026-07-19 | `2aea276` | `python -m pytest -q` from zero | GREEN: 649 passed in 252.70s; no retry or result splicing. |
| 2026-07-19 | `2aea276` | `python scripts/stress_actor_transport.py --generations 10000 --requests 100000 --pool-size 4 --concurrency 100 --close-samples 100 --heartbeat-requests 1000 --idle-seconds 0.5 --resource-rounds 8 --resource-warmup 3 --resource-generations 50 --output artifacts/actor-stress-f10-2aea276.json` | PASS: 10,000/100,000 unique; all ownership/error counters 0; max active 4; post-close broker/push/Actor resources 0; heartbeat business windows 0/0; throughput ratio 1.005572; resources 191 x8 exact plateau. Artifact SHA256 `17A2FBBD54BE04E9F609200A74490F0D7C5757D653CB1A7D78B31F642B18A70A`. |
| 2026-07-19 | working tree after review | deterministic heartbeat sendall-to-close interleaving | GREEN: heartbeat blocks behind phase wire lock; total count increases after final response, business-window count remains 0. |

## Known Failures And Risks

- Historical clean full suite at `5925c02`: `1 failed, 645 passed`; heartbeat total counter included one legal post-business/pre-disable heartbeat.
- P1-A and P1-B have RED and focused GREEN proof; broader matrix is pending.
- Plan revision 1.4 must not change after F10-SPEC is committed; Result must use the committed blob SHA256.
- No retry has been used to hide a failure. Network retry count: 0.
