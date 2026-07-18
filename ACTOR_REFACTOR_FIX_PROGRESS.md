# Actor Fatal Reason Final CI Correction Progress

- Branch: `actor-transport-refactor`
- Reopened final HEAD: `5bcc76879efc0a102dfc37a100d63cd6c43be4f2`
- Production source and heavy evidence source: `a987c163015ed297066817a937d4f4ed046ec874`
- Draft PR: https://github.com/electkismet/eltdx/pull/12, OPEN/Draft/unmerged.
- User-owned dirty paths at reopen: none.
- Current unique `in_progress`: commit and push the corrected thread-exit observation, then wait new exact-head CI/Pages.
- Last completed: exact-head Pages run 29644585782 SUCCESS; exact-head CI run 29644585790 failed only Windows Python 3.13 job 88080598193.
- Failure signature: `test_failed_actor_exits_without_waiting_for_pool_owned_locks[broker]` observed `runtime.stopped` set while `Thread.is_alive()` was transiently true; log thread repr was already `stopped`; run result 1 failed / 637 passed in 242.47s.
- Root cause: `runtime.stopped` is set by the Actor before the thread target returns, so Event publication cannot itself prove `Thread.is_alive() == False` on the immediately following instruction.
- Correction: use one absolute `monotonic()+0.2s` budget for both `runtime.stopped.wait()` and a join using only the remaining budget. The gate remains 0.2s total; no timeout is widened and no assertion is removed.
- Verification: 20 independent processes each passed the four blocked-owner cases; log `C:\Users\ax\Desktop\eltdx\artifacts\actor-thread-exit-20proc-5bcc768.log`, SHA256 `65FEF7CF27E26DE0032452DC2054E46712547668A813B01322C5F169FA6D86E7`.
- Verification: lifecycle regression file 92 passed in 7.55s; transport matrix 428 passed in 16.14s; complete pytest 638 passed in 255.01s (0:04:15).
- Next exact action: commit/push; wait new exact-head CI/Pages; perform three final reviews; update result and delete this ledger in the next FINAL commit.
- Workflow: this correction does not modify workflow files.
