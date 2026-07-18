# Actor Fatal Reason Reopened Fix Progress

- Branch: `actor-transport-refactor`
- Reopened FINAL identity: `d38bfc6bd2d30336e2a222b3ee2a6cb2b366edec`
- Current RED test commits: `ee077cc` (`Fix-Checkpoint: F08-RED`), `12ea212` (`Fix-Checkpoint: F08-RED2`)
- Draft PR: https://github.com/electkismet/eltdx/pull/12, OPEN/Draft/unmerged.
- Reason for reopening: final independent review found unresolved Guard fatal identity fallback and no-error deferred Push drain.
- RED evidence on unchanged production: 3 targeted tests failed (`waits_for_epoch_reason_cell`, `owner_failure_selection`, `unadorned_abandon`); retirement baseline was 40 passed.
- Production correction: resolver owner selection is sticky; Guard runtime fatal fallback is disabled while a resolver exists; deferred Push abandon drains on owner paths without treating `publish_close(None)` as abandon.
- Focused verification: targeted corrections 3 passed; complete retirement regressions 43 passed in 0.36s.
- Current unique `in_progress`: commit/push F08 production correction, run transport/lifecycle/full/package/docs evidence, then rerun stress/performance only if source identity changes, update result ledger, delete this file in a new FINAL commit, push, wait exact-head CI/Pages, and perform three final reviews.
- Workflow: this reopened correction does not modify workflow files.
