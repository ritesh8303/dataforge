# Interview / Loom demo script (~2–3 minutes)

Record this once; reuse for applications and thesis supervisors.

## Setup (30 seconds, before record)

1. Open: https://ritesh8303.github.io/dataforge/
2. Have ready: Dashboard, Jobs, Agent (Career Matching), About
3. For hybrid Match on Agent page, append your key (never show it on camera):
   `docs/agent.html?api_key=YOUR_KEY`  
   Key lives in repo-root `aws-keys-do-not-commit.txt` (gitignored).

## Script

### 0:00–0:25 — Problem
> “Public EU job boards are noisy for interns and juniors. Visa and English-OK signals are buried in long JDs. DataForge turns free sources into a lakehouse and an advisory match API.”

Show: Home → About (problem → pipeline → AI).

### 0:25–1:00 — Lakehouse
> “Daily ingest into Bronze, Silver SCD Type 2, Gold marts on S3 in Frankfurt. Terraform, CI, quality gate, dbt on Gold. Jobs and Metrics APIs power the Pages UI.”

Show: Dashboard KPIs → Jobs board with a filter (e.g. Berlin / entry).

### 1:00–1:50 — Match
> “Match API is FastAPI on Lambda: hybrid BM25 + dense retrieval, PII redaction, visa-aware filters, citations per job_id. Multi-agent mode is optional and budget-capped.”

Show: Agent wizard → hybrid (or agent) → one result card with citations.  
Say: “Advisory only — not legal advice on visas.”

### 1:50–2:20 — Frugal GenAI / thesis
> “Rules-first enrichment, Bedrock Nova Micro when quota allows, kill switch and daily budget. On 103 labelled queries, dense retrieval leads nDCG@10 at about 0.22 versus about 0.15 for the keyword wizard. Bedrock RQ2 is still waiting on AWS quota; we can measure the same Pareto with gpt-4o-mini for cents while residency is scored separately.”

Show briefly: GitHub `evals/results/eval_report.md` or RESULTS_DRAFT.

### 2:20–2:45 — Close
> “Same repo is the portfolio and the UE Potsdam MSc reference implementation. Cost target €0–25 typical, €40 hard cap.”

End on Dashboard or About.

## Do not say on camera
- API key value  
- Exact AWS account IDs unless asked  
- “Production hiring decisions” — we rank public JDs for seekers, not employer ATS screening

## After recording
Upload to Loom/unlisted YouTube; paste link in CV / applications / thesis appendix.
