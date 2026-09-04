# AI Layer Architecture (Thesis Extension)

This document describes the AI-in-production layer added for the MSc thesis.

## Components

| Module | Path | Role |
|--------|------|------|
| Model Gateway | `src/ai_gateway/` | Multi-provider router (OpenAI, Anthropic, Bedrock, local) |
| Batch Enrichment | `src/enrichment/` | LLM structured extraction → `ai_job_enrichment.csv` |
| Embedding Index | `src/embedding_index.py` | Vector index for semantic matching |
| Match API | `src/match_api.py` | Resume→job ranking endpoint |
| Enrichment Lambda | `src/enrichment_handler.py` | Scheduled post-Gold enrichment |
| Eval Harness | `evals/` | Thesis experiment scripts |

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `AI_ENABLED` | `true` | Master kill switch |
| `AI_ENRICHMENT_ENABLED` | unset | Enable enrichment in gold_generator |
| `AI_ENRICHMENT_SAMPLE_RATE` | `1.0` | Fraction of jobs to enrich (cost control) |
| `OPENAI_API_KEY` | — | OpenAI provider |
| `ANTHROPIC_API_KEY` | — | Anthropic provider |
| `AWS_BEDROCK_REGION` | `eu-central-1` | Bedrock region |

## Deployment

```bash
cd terraform
terraform apply  # includes ai.tf (enrichment + match API)
```

After apply, paste `match_api_url` output into `docs/agent.html` as `MATCH_API_URL`.

## Local testing

```bash
# Run eval suite (no API keys required — uses local provider)
python evals/run_matching_eval.py
python evals/run_enrichment_eval.py
python evals/run_router_eval.py
python evals/run_roi_model.py

# Unit tests
pytest tests/test_ai_gateway.py tests/test_embedding_index.py -v
```

## Guardrails

- LLM output never mutates `job_id` or SCD keys
- Enrichment writes **additive** Gold columns only
- JSON schema validation with local fallback
- Cost/latency logged per call via `CostLogger`
