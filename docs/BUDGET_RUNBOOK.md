# Budget runbook — DataForge (€0–25 typical / €40 hard cap)

Region: **eu-central-1**. Idle target after thesis: **€0/month** (turn off AI layer).

## Daily / weekly checks

1. AWS Budgets + billing alarms (`terraform/budget.tf`, `cost_monitoring.tf`).
2. CloudWatch estimated charges alarms.
3. Match reserved concurrency = **2** (when `ai.tf` applied).
4. `AI_ENABLED=false` kill switch via Lambda env / SSM if spend spikes.
5. `AI_DAILY_BUDGET_USD` on Match/enrichment (default low single digits).

## Enable AI layer safely (OpenAI-first)

Production GenAI path is **OpenAI** (`gpt-4o-mini` + `text-embedding-3-small`) for Match,
enrichment, ingest-review, and agent rerank/explain. Local heuristics remain the offline fallback.

```bash
# 1) Store the key (do not commit)
aws ssm put-parameter \
  --name /dataforge/openai_api_key \
  --type SecureString \
  --value "sk-..." \
  --overwrite \
  --region eu-central-1

# 2) Terraform (openai_api_key optional if SSM is populated)
cd terraform
# set openai_api_key / match_api_key in tfvars OR rely on OPENAI_API_KEY_SSM
terraform plan
terraform apply

# 3) Redeploy Lambda code
py -3 scripts/deploy_lambdas.py
# also update dataforge-match-api + dataforge-enrichment if not in that list
```

Immediately after apply:

- Hit Function URL `/health` once.
- Run one Match with local-sized limit; confirm `cost_summary.by_provider` includes `openai`.
- Confirm Langfuse (if configured) + S3 `llm_traces/` (if wired).

Kill switch: `AI_ENABLED=false` on Match / enrichment / Gold.

## Disable AI layer (idle / thesis done)

1. Set `AI_ENABLED=false` on enrichment + match Lambdas **or**
2. `terraform destroy` targeted on AI resources / rename `ai.tf` back to `ai.tf.optional` and apply.
3. Keep lakehouse crons if you still want Gold; or pause EventBridge schedules.

## What burns money

| Item | Risk | Control |
|------|------|---------|
| Bedrock enrich full corpus | High | Rules-first + sample rate |
| Multi-agent Match | Medium | max 4 LLM calls, concurrency 2 |
| Frontier providers on full corpus | High | RQ2 sample only (`--live-providers` + limit) |
| Always-on containers / RDS / OpenSearch | Out of scope | Do not add |
| Dual SFN + Lambda cron triggers | Double compute | `enable_sfn_schedule=false` |

## Thesis logging

```bash
py -3 scripts/roi_report.py
py -3 evals/run_rq2_pareto.py
py -3 evals/run_agent_ablation.py
```

Keep total logged GenAI spend under **€40**. Document FX as approximate USD→EUR in ROI report.
