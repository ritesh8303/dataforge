# Portfolio + thesis packaging (CV / Loom / colloquium)

**Last update:** 2026-09-30  
Use with [`DEMO_SCRIPT.md`](../DEMO_SCRIPT.md), [`ADMIN_CHECKLIST.md`](ADMIN_CHECKLIST.md), [`RESULTS_DRAFT.md`](RESULTS_DRAFT.md).

---

## Honest CV bullets (copy/adapt)

- Built a live **EU early-career data/AI job lakehouse** on AWS (multi-source ingest → Bronze / Silver SCD Type 2 / Gold) with Terraform, CI quality gates, and dbt on Gold — product board scoped to **fresher / working-student / internship / thesis** roles.
- Shipped a **FastAPI Match API** (Lambda Function URL): hybrid BM25 + dense retrieval, PII redaction, visa-aware filters, per-`job_id` citations; optional multi-agent + MCP tooling.
- Ran an **honest labelled matching eval** (103 queries × 96 jobs): dense nDCG@10 **0.22** vs heuristic wizard **0.15**; unit-cost model ≈ **€0.06 / 1k jobs** under rules-first enrichment.
- Designed a **frugal multi-provider GenAI gateway** (Bedrock-first, OpenAI/Anthropic/local fallback, kill switch + daily budget); documented AWS Nova Micro quota block and measured RQ2 via alternate providers when needed.

**Do not claim:** nightly Bedrock enrichment is running; live Bedrock RQ2 Pareto is complete — until quota or OpenAI/Anthropic cells are recorded in `RESULTS_DRAFT.md`.

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
| RQ2 OpenAI/Anthropic Pareto (~€0.01) | Ready — needs `OPENAI_API_KEY` / Anthropic key |
| LanceDB URI on Match (`VECTOR_STORE_URI`) | Wired in code + `terraform/ai.tf`; JSON index remains fallback |
| Bedrock Nova Micro quota case `178955627400906` | Still `CASE_OPENED` as of 2026-09-30 |
