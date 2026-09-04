# Roadmap — industry + UE thesis

Aligned with Germany/EU 2026 JDs (applied AI + data platforms) and the UE M.Sc. Data Science thesis.

**Rule:** ship the next row before adding new tools. Depth beats breadth.

## Thesis (UE Applied Sciences, Potsdam)

Working title (see `docs/thesis/THESIS_EXPOSE.md`):

> Integrating generative AI into an existing production medallion pipeline — architecture, evaluation, cost — case study: DataForge.

Academic core that is **already live:** multi-source ETL, Pydantic gates, SCD Type 2, Gold quality metrics, Terraform, CI.

Academic extension **in progress:** hybrid quality (rules + LLM), evals vs baselines, cost/latency. Not complete, not on GitHub `main` until it is honest.

## Industry requirements → DataForge

| JD cluster | Requirement | DataForge status |
|---|---|---|
| Data platform / DE | Medallion + SCD2 + IaC + CI | **Shipped** (AWS) |
| Analytics engineering | Real dbt + tests in CI | **Shipped locally / CI** (`dbt/`) |
| BI / metadata | Gold dictionary + quality gate | **Shipped** (`docs/DATA_DICTIONARY.md` + quality script) |
| Applied GenAI | RAG, evals, agents, guardrails | **Integrating** (local `src/ai_gateway`, `evals/`) |
| Cloud / DevX | Docker one-command demo | **Partial** (analytics image, not full lakehouse) |
| Governance | EU AI Act, logging, cost | **Planned** (1-pager + traces) |

## Sequence (do in order)

1. Keep AWS lakehouse green (do not break ingest).
2. dbt + quality gate on Gold (this slice).
3. Commit AI layer only with WIP labels + real eval numbers (not nDCG=1.0 on 3 fake IDs).
4. Persisted vector DB (pgvector or Chroma), not JSON-only embeddings.
5. One LangGraph tool loop + failure tests.
6. Docker: clone → Gold analytics in &lt;2 min, then expand.
7. Freeze scope; write thesis on measured results.

## Explicitly out of scope until internships convert

Training foundation models, Kubernetes deep dive, Kubeflow, weekly new agent frameworks.
