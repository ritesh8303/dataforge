# Data sourcing policy

DataForge ingests **public job postings only** (no CVs, no personal contact data).
All sources are free. Attribution is stored on each Bronze/Silver row when provided.

## Active sources

| Source id | Access | Cadence | Attribution / licence notes |
|-----------|--------|---------|-----------------------------|
| `arbeitnow` | Public API, no key | Daily Lambda | Keep `visa_sponsorship` / `relocation` / `english_speaking` tags |
| `ba_api` | BA Jobsuche public REST | Daily Lambda | Official German public employment service |
| `eures` | EURES public search API | GitHub Actions | EU public job mobility portal |
| `berlin_startups` | Public RSS | Daily Lambda | Berlin Startup Jobs feeds (tech categories) |
| `direct` | Public ATS feeds (Greenhouse, Lever, Ashby, Workable, SmartRecruiters, Recruitee, Personio XML, Workday, Pinpoint, Comeet) | Daily Lambda | Company career pages; DACH-focused board list in `config/sources/dach_ats.json` + Personio tenants in `config/sources/personio_tenants.json` |
| `himalayas` | Public JSON API | Optional / scheduled | [Himalayas API](https://himalayas.app/api) — link back required |
| `hn_whoishiring` | Algolia HN Search API | Optional / scheduled | Hacker News “Who is hiring”; EU/DE tech filter only |

## Dedup and tech scope

- Cross-source identity: semantic `job_id` = `sem_{company}_{title}_{location}`; `dedup_key` adds a description hash fragment; `source_attribution` lists all contributing sources.
- **AI-first classification** (`enrichment.ai_classify` + enrichment Lambda): field, seniority, visa, languages via `gpt-4o-mini`. Rules (`rules_de_en`) are **fallback only** when AI is disabled or the call fails / audience live-call budget is exhausted. Silver no longer stamps rule labels.
- **Product audience gate** (`processing.audience_gate`): Gold / Jobs / Match publish only **EU** + **data/AI-related fields** + **fresher / working_student / internship / thesis**. Prefers `ai_*` enrichment columns; otherwise live AI classify (cap `AUDIENCE_AI_MAX`). Ambiguous rows go to `data/hitl_ingest_review_queue.csv`.
- **Ingest review agent** (`agent.ingest_review_agent`): auto-reviews that queue (optional LLM). Writes `data/hitl_ingest_review_decisions.csv`; accepts override the gate. Run: `py -3 scripts/run_ingest_review_agent.py` (add `--llm` for model assist). Gold runs the agent automatically when `INGEST_REVIEW_AGENT=true` (default).
- **Working-student coverage (no scrape of LinkedIn/StepStone/workingstudentjobs.de):** expanded BA Jobsuche Werkstudent/Praktikum/thesis queries, EURES working-student keywords, and student-heavy DACH ATS boards in `dach_ats.json` (BMW, Mercedes-Benz, DB, Personio, ABOUT YOU, CHECK24, etc.).
- **ATS expansion:** `config/sources/dach_ats.json` + `personio_tenants.json` (packaged under `src/config/sources/` for Lambda) — Greenhouse, Ashby, SmartRecruiters, Recruitee, Workable, Personio XML. Dead tokens pruned at fetch time.

## Hard no

LinkedIn, Indeed, StepStone, Xing, Google Jobs — no free API and ToS forbids scraping.  
Do not scrape workingstudentjobs.de (`/api/` disallowed in their robots.txt).

## Politeness

- Prefer official/public JSON/XML feeds over HTML scraping.
- Personio XML: ~200 ms delay between tenants, daily cadence, ETag/cache when available.
- Honour `robots.txt` for any future JSON-LD career-page connector.
- Store posting content only; never scrape candidate profiles.

## Saved-search alert stub

Check for new product-board jobs (local `data/gold` or `GOLD_BUCKET`) and optionally POST to a webhook:

```bash
py -3 scripts/check_new_jobs_alert.py --employment working_student --field data_engineering --city Berlin
# Optional: set ALERT_WEBHOOK_URL=https://hooks.example/…
```
