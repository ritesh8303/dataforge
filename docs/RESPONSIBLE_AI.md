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
- Local-provider ablation can score citation *structure* highly; use `evals/run_faithfulness_spotcheck.py` (and optional OpenAI judge) for semantic grounding checks.
- Multi-agent path is bounded (≤4 LLM calls, ≤6 handoffs) — not an unbounded agent swarm.
- Rules classifier and LLM enricher can disagree; rules-first is the cost control, not ground truth.
- Match nDCG in CI uses **synthetic** labels; public claims should cite `evals/data/human_label_protocol.md` when human grades exist.
- Product UI headlines use the **EU data/AI early-career gate**; lakehouse volume is secondary research context.
- Link health probes (`scripts/check_apply_links.py`) demote dead URLs; “Active” still means “seen in source feeds,” not a guarantee the employer is accepting applications today.
- Seeker feedback (closed / spam / wrong location) is stored locally in the browser until an export/webhook is wired.

## Trust chrome (product)

| Signal | Meaning |
|--------|---------|
| `trust_tier=verified` | Direct company ATS feed |
| `trust_tier=aggregator` | BA / EURES / Arbeitnow / Berlin startups |
| `trust_tier=uncertain` | Ambiguous audience classification / HITL |
| `trust_tier=stale` / `dead` | Past freshness SLA or failed apply-link probe |
| `english_jobs` headline | `language_requirement=english_only` only |

## References in-repo

- Kill switch / budgets: `src/ai_gateway/`
- Citations + filters: `src/api/match_service.py`
- Multi-agent critic: `src/agent/nodes.py`
- Sources & licences: `docs/DATA_SOURCING.md`
