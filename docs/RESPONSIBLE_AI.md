# Responsible AI — DataForge Match & enrichment

Advisory product for **job discovery**. Not a hiring decision system, not legal advice on visas or immigration.

## EU AI Act mapping (employment)

| Obligation / risk area | DataForge control |
|------------------------|-------------------|
| Transparency | UI + API disclaimer; OpenAPI docs; cited `job_id` + evidence snippets |
| Human oversight | Critic node; HITL queue when confidence &lt; 0.7 or citation fail (`src/agent/hitl.py`) |
| Accuracy / robustness | Honest evals (nDCG/MRR/F1); CI gates; kill switch / daily € budget |
| Data governance | Medallion lineage; `docs/DATA_SOURCING.md`; PII redaction before LLM (`src/security/pii.py`) |
| High-risk employment systems | **Out of scope as an employer ATS.** We do not score candidates for hiring decisions; we rank public JDs for a job-seeker |

If this product were embedded in an employer screening tool, additional conformity assessment would be required — that is explicitly **not** the deployment mode.

## GDPR

- Prefer processing **public job postings**, not CVs at rest.
- Resume text is processed **in-request**, PII-redacted before provider calls, not written to Gold by default.
- HITL review payloads may contain redacted resume excerpts — store under restricted S3 prefix, short retention.
- Right to erasure: delete local HITL CSV / S3 review objects; no CV warehouse in the lakehouse.

## Visa / immigration sensitivity

Fields such as `ai_visa_stance`, `job_seeker_visa_friendly`, and profile `visa_status` are **heuristic signals** from JD text and user self-report.

- Always show evidence quotes where available.
- Never hide jobs by default based on visa flags.
- UI/API copy: “not legal advice.”
- Treat immigration status as sensitive; do not log raw visa answers to public metrics.

## Limitations (honest)

- Majority of JDs have `ai_visa_stance = not_mentioned`.
- Local-provider ablation can score citation *structure* highly while Bedrock faithfulness still needs spot-check.
- Multi-agent path is bounded (≤4 LLM calls, ≤6 handoffs) — not an unbounded agent swarm.
- Rules classifier and LLM enricher can disagree; rules-first is the cost control, not ground truth.

## References in-repo

- Kill switch / budgets: `src/ai_gateway/`
- Citations + filters: `src/api/match_service.py`
- Multi-agent critic: `src/agent/nodes.py`
- Sources & licences: `docs/DATA_SOURCING.md`
