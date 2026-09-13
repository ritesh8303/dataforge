# Terraform notes (DataForge)

## Apply order

1. Core lakehouse: `main.tf`, `metrics.tf`, budgets — already live in `eu-central-1`.
2. Orchestration thin proof: `stepfunctions.tf` (Express Silver→Gold). Schedule **off** by default (`enable_sfn_schedule = false`).
3. AI layer (cost): rename `ai.tf.optional` → `ai.tf`, set `match_api_key`, then `terraform plan/apply`.

```bash
cd terraform
cp ai.tf.optional ai.tf   # enables enrichment + Match Function URL
terraform plan
# terraform apply   # only when ready for Bedrock spend under €40 cap
```

## Cost guardrails

- Match reserved concurrency = 2
- Budgets / billing alarms in `budget.tf` / `cost_monitoring.tf`
- Kill switch + daily € budget in Match/enrichment env vars
- Do not enable `enable_sfn_schedule` and keep all ingest crons unless you intend dual triggers

## Outputs of interest

- `medallion_state_machine_arn` — Step Functions Express
- `match_function_url` / `match_api_url` — only after `ai.tf` is applied
