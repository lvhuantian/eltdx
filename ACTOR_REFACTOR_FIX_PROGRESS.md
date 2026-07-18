# Actor Refactor Fix Progress

- Fix-Checkpoint: F09-FIX
- Branch: `actor-transport-refactor`
- Starting delivery HEAD: `7149406abd566d5d332279e896911fedbe391910`
- Starting production SHA: `3589a09095c21908dd738e266e295393b91548e8`
- Local/remote/PR HEAD at start: `7149406abd566d5d332279e896911fedbe391910`
- Worktree at start: clean
- Stage: stress and paired performance evidence complete
- Test results: targeted fatal/retirement/Push/Pool suites: `231 passed in 9.56s`. Three new race tests run in 20 independent pytest processes (60 test cases): `failed=0`; log `artifacts/actor-f09-20proc-45e6703.log`, SHA256 `49400E70509F823A86DE2B5186F573733676E9F4F63574223E39E85CB2B08F62`.
- Stress: `artifacts/actor-stress-f09-f117871.json`, source `f1178712bf108d113db7a345f53d3a9e9e0d113b`, SHA256 `F3A574D9E1DDEEC690C24CDB967AE732863243D2FCDCDC02268B034EF78CAECD`; 10k/100k unique, all duplicate/missing/cross counters zero, max active 4, leases/waiters/pins/frames/bytes/Actor threads zero, measured resources 190 x8 plateau.
- Paired performance: declaration `7ec426845f5dd3c73d69c781ac11c49836955e333507128a19d973ef5fe540e5`; bundle `artifacts/perf-f09-f117871/campaign_bundle.json` SHA256 `043D6306C303481E8AB2052AFCF180CB3BD32EBC3DC5DD9D26F68C83963EEB87`; verifier report SHA256 `4DCFF1CF6C5A6AABA07E256492138EFEECFAC5679C8BA9BA33C5EC2B7E85951B`; result `FAIL, user-approved exception` (throughput ratios 0.918/0.927; no integrity errors; no new exception).
- Next: full pytest, package wheel/sdist + twine check, MkDocs strict, then update permanent Result/Plan evidence and final review.
- Pending push: none
