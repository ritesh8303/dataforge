# Decisions — live-only path (24 Oct 2026)

## Data

| Item | Value |
|------|--------|
| Live pull | 11,445 jobs from Jobs API |
| Snapshot | 120 data/tech jobs; descriptions backfilled from `job_url` (was gold 300-char cap) |
| Description cap | Gold board/API now keeps **4000** chars (`gold_generator`); old 300 cut company intros only |
| Queries | 50 new |
| Labels | `author_confirmed_rules_v3_live` (rule-based on live text) |
| Forbidden for main table | Old `sample_jobs.json` / pilot nDCG 0.1462 / 0.1258 / 0.1531 |

## Methods

- wizard 60/20/20  
- TF-IDF  
- semantic = OpenAI `text-embedding-3-small` when `OPENAI_API_KEY` is set  

## Current numbers file

`evals/results/final_matching_eval.md` — wizard/TF-IDF are valid on live labels; “semantic” column is **local smoke** until OpenAI key is available.
