# cb-chart: what this branch carries on top of upstream main

Branch `cb-chart` of project-cb-lab/llm-d-router is upstream `llm-d/llm-d-router` main plus the chart
changes below, built and published to `oci://us.icr.io/llmd/charts` by curvebender-tools'
`build-router.yaml` (version `<release>-sha.<short>`).

Rules: every commit here is also an open upstream PR; rebase onto upstream main at least every two
weeks; drop a commit once its PR merges. When this list is empty, switch to an upstream release.

| Commit | Upstream PR | What |
| --- | --- | --- |
| feat(chart): pin router images by digest | llm-d/llm-d-router#4497 | `image.digest` on EPP, tokenizer and latency-predictor images |
