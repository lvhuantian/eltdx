# Actor Refactor Fix Progress

- Fix-Checkpoint: F09-RED
- Branch: `actor-transport-refactor`
- Starting delivery HEAD: `7149406abd566d5d332279e896911fedbe391910`
- Starting production SHA: `3589a09095c21908dd738e266e295393b91548e8`
- Local/remote/PR HEAD at start: `7149406abd566d5d332279e896911fedbe391910`
- Worktree at start: clean
- Stage: deterministic RED reproduction complete
- Test results: old production code fails 2 new tests; stale-None returns `None` while Push raises the exact error, and diagnostics returns `RUNNING` after fatal publication. Command: `python -m pytest -q tests/test_transport_retirement_regressions.py::test_guard_failure_linearizes_after_none_snapshot_when_fatal_publishes tests/test_transport_lifecycle_regressions.py::test_diagnostics_does_not_report_running_after_fatal_publication --maxfail=2` (2 failed, expected RED).
- Next: review lock order/P2 call graph, then implement minimal Guard slow-path recheck and rerun targeted tests.
- Pending push: none
