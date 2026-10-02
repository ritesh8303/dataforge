# Portfolio + thesis packaging (CV / Loom / colloquium)

**Last update:** 2026-10-02  
Use with [`DEMO_SCRIPT.md`](../DEMO_SCRIPT.md), [`ADMIN_CHECKLIST.md`](ADMIN_CHECKLIST.md), [`RESULTS_DRAFT.md`](RESULTS_DRAFT.md).

---

## Honest CV bullets (copy/adapt)

- Built a live **EU early-career data/AI job lakehouse** on AWS (multi-source ingest → Bronze / Silver SCD Type 2 / Gold) with Terraform, CI quality gates, and dbt on Gold — product board scoped to **fresher / working-student / internship / thesis** roles.
- Shipped a **FastAPI Match API** (Lambda Function URL): hybrid BM25 + dense retrieval, PII redaction, visa-aware filters, per-`job_id` citations; optional multi-agent + MCP tooling; LanceDB with JSON index fallback.
- Ran an **honest labelled matching eval** (103 queries × 96 jobs): packaging semantic-embed dense nDCG@10 **0.22** vs heuristic wizard **0.15**; CI local-tfidf pin reports dense **0.13** (named separately).
- Designed a **frugal multi-provider GenAI gateway** (OpenAI-first while Bedrock quota is 0; kill switch + daily budget). Live RQ2: **gpt-4o-mini 40/40** (~$0.0011); nightly OpenAI enrichment sample rate **0.25**. Modelled unit cost ≈ **€0.10 / 1k jobs**.

**Do not claim:** nightly **Bedrock** enrichment is running; live Bedrock RQ2 Pareto is complete. Those remain blocked until Nova Micro quota is approved.

---

## Links for applications

| Asset | URL / path |
|-------|------------|
| Live dashboard | https://ritesh8303.github.io/dataforge/ |
| Repo | this GitHub repo |
| Demo script | `docs/DEMO_SCRIPT.md` (~2–3 min Loom) |
| Colloquium slides | `docs/thesis/COLLOQUIUM_SLIDES.md` + `latex/colloquium_slides.tex` |
| Results tables | `docs/thesis/RESULTS_DRAFT.md` |

---

## Thesis admin (you still do manually)

1. Email Erst + Zweit supervisors — template in `ADMIN_CHECKLIST.md`
2. Paper form + CampusNet (title freezes)
3. Rewrite LaTeX drafts in your own voice (UE AI-writing rule)
4. Practice colloquium with timing table in `COLLOQUIUM_SLIDES.md`

---

## Engineering follow-ups (optional)

| Item | Status |
|------|--------|
| RQ2 OpenAI Pareto (40/40, ~$0.0011) | **Done** 2026-10-01 |
| Nightly OpenAI enrichment (sample 0.25) | **Live** |
| LanceDB URI on Match (`VECTOR_STORE_URI`) | Wired in code + `terraform/ai.tf`; JSON index remains fallback |
| Bedrock Nova Micro quota case `178955627400906` | Still `CASE_OPENED` as of 2026-10-01 |
