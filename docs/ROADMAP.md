# Roadmap — living industry portfolio (+ UE thesis packaging)

**Primary goal:** DataForge is a hireable portfolio (DE + applied GenAI).  
**Secondary:** same artifacts feed the UE M.Sc. thesis — thesis does **not** shrink the checklist.

**This file is not a contract.** Waves below are the *current best bet* from Sep 2026 market research + your JD set. Reorder, drop, or add items when industry demand or your target roles change. Prefer editing this file over following a stale sequence.

## Operating rules

1. **Depth over FOMO** — finish or explicitly defer the current slice before adding a new tool.
2. **Roadmap follows the market** — when JDs or hiring blogs shift, update this file and the coverage canvas; do not treat Wave numbers as compulsory.
3. **Thin proof ≠ forgotten** — if a keyword matters for interviews but not for product depth, add a thin proof (one DAG, one adapter, one design note) instead of a rewrite.
4. **Honest status** — README / CV only claim Covered items; Partial / Must add stay labeled until shipped.
5. **Stay current during build** — roughly every 2–4 weeks (or after a cluster of new applications): skim 5–10 target JDs + one market write-up, then patch this roadmap if anything material changed.

**How to change the plan:** edit the tables/waves here, note `Last market check:` date below, and adjust build order in the next coding session. No approval ritual required.

**Last market check:** 2026-09-13  
**Sources (seed):** DACH DE/AI JDs (Wavestone, GenAI DE, RAG/agent roles), EU DS hiring notes, your `UPSKILLING_PLAN` P0–P2 list, Allianz Agentic AI WS JD, workingstudentjobs.de teardown.

Full gap matrix: Cursor canvas `industry-portfolio-coverage` (filter **Gaps only**) — update that when coverage status changes.

## Industry coverage (honest — Sep 2026)

| Cluster | Status |
|---|---|
| Medallion + SCD2 + multi-source ETL + Terraform + CI | **Covered** (live AWS) |
| dbt + dictionary + quality gate | **Partial** (add dbt docs polish) |
| Tech-only taxonomy + visa/seniority rules + DACH/Personio/Himalayas/HN sources | **Partial** (Phase A shipped in code; AWS apply pending) |
| RAG Match / enrichment / multi-provider gateway | **Partial** (Bedrock-first + kill switch + prompt registry in code) |
| Hybrid BM25+vector (LanceDB), honest evals CI gate | **Partial** (Phase A shipped; LanceDB optional dep) |
| FastAPI Match API + PII + citations + visa profile + agent toggle | **Partial** (Phase B shipped in code; AWS apply pending) |
| MCP, multi-agent | **Partial** (Phase D in code: sequential/LangGraph, HITL, MCP, ablation; AWS apply pending) |
| Airflow keyword, Azure provider stub, streaming/Spark talk-tracks | **Thin proof** (compose Airflow DAG + STREAMING_AND_SPARK + Step Functions Express) |
| Foundation training, K8s/Kubeflow, unbounded agent swarm | **Skip OK** |

## Current suggested waves (editable)

Suggested order only — swap freely if a target role weights differently.

**Execution order locked for this build:** A → C → B → D → E → F (AWS-frugal plan).

### Wave 1 — GenAI hire gaps

1. Keep AWS lakehouse green.
2. Real eval gold set + CI regression (no fake nDCG=1.0). **Done in code (Phase A).**
3. Persisted vector store: **LanceDB on S3** (not RDS/pgvector). **Done in code.**
4. Hybrid retrieval (BM25 + dense + RRF). **Done in code.**
5. FastAPI Match/Jobs + OpenAPI + API key / rate limit. *(Phase B)*
6. Resume PII redaction + cited `job_id` explanations. *(Phase B)*
7. Prompt registry + cost budgets / kill switch. **Done in code (Phase C).**
8. LangGraph multi-agent (Supervisor + specialists) + failure tests. **Done in code (Phase D).**
9. Langfuse Cloud free tier (EU) + S3 traces. **Tracing helpers in code (Phase C).**
10. Thin MCP server (2–3 tools over Jobs/Match). **Done in code (Phase D).**

### Wave 2 — DE keywords without a rewrite

1. One Airflow DAG documenting Bronze→Silver→Gold→enrich (local Docker only; keep EventBridge + Step Functions on AWS). **Done (compose profile + DAG).**
2. Compose demo: analytics + Match API + Airflow. **Done (`docker-compose.yml` profiles).**
3. Azure OpenAI as gateway provider stub (DACH literacy). **Stub added.**
4. Streaming + Spark: design notes — not a rebuild. **Done (`docs/STREAMING_AND_SPARK.md`).**
5. dbt docs + README coverage table (Covered / Partial / Planned). **dbt docs generate in CI.**
6. EU AI Act / GDPR 1-pager. **Done (`docs/RESPONSIBLE_AI.md`).**
7. Step Functions Express Silver→Gold. **Done (`terraform/stepfunctions.tf`; schedule off by default).**
8. Rename/apply `ai.tf` when ready for Bedrock spend. *(operator step — not auto-applied)*

### Wave 3 — packaging (portfolio + thesis)

1. Unit-economics page from real cost logs. **`scripts/roi_report.py`**
2. Demo script / short loom for interviews. *(operator)*
3. Thesis write-up on measured results (`docs/thesis/RESULTS_DRAFT.md`). **Drafted**
4. UI polish (KPIs, visa/entry filters, About). **About + agent filters; dashboard KPIs live**
5. Budget runbook + README honesty pass. **Done**
## Change log (roadmap pivots)

| Date | Change | Why |
|---|---|---|
| 2026-09-04 | Portfolio-first waves; hybrid RAG, Langfuse, FastAPI, MCP promoted | Industry audit vs thesis-only framing |
| 2026-09-13 | AWS-frugal redesign (LanceDB, Function URL, Bedrock-first, €40 cap); tech-only + visa-aware audience; free source expansion; Phase A+C started | Cost + JD coverage + job-seeker-visa use case |
| 2026-09-13 | Phase B: FastAPI+Mangum Match API, PII, citations, visa filters, agent.html hybrid toggle, Function URL in ai.tf.optional | Portfolio Match surface |
| 2026-09-13 | Phase D: multi-agent graph, HITL, MCP, ablation; Phase E: Step Functions Express, compose Airflow, RESPONSIBLE_AI, Spark note, dbt docs CI | Thesis-plus orchestration + governance |
| 2026-09-16 | Demo script + RESULTS honesty; MCP auto-loads gitignored Match key; Bedrock Nova TPM increase requested (PENDING) | Wait on quota without blocking portfolio demo |

## Explicitly out of scope (until evidence says otherwise)

Training foundation models, Kubernetes deep dive, Kubeflow, weekly new agent frameworks, second portfolio app.  
If a concrete JD you are applying to requires one of these, move it into a wave with a thin or deep proof — do not ignore the JD to protect this list.
