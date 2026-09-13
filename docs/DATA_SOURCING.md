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

## Hard no

LinkedIn, Indeed, StepStone, Xing, Google Jobs — no free API and ToS forbids scraping.

## Politeness

- Prefer official/public JSON/XML feeds over HTML scraping.
- Personio XML: ~200 ms delay between tenants, daily cadence, ETag/cache when available.
- Honour `robots.txt` for any future JSON-LD career-page connector.
- Store posting content only; never scrape candidate profiles.

## Dedup and tech scope

- Cross-source identity: semantic `job_id` = `sem_{company}_{title}_{location}`; `dedup_key` adds a description hash fragment; `source_attribution` lists all contributing sources.
- Tech pre-filter: `enrichment.rules_de_en.classify_field` sets `is_tech` / `field_rule` in Silver. Gold marts default to tech roles. `non_tech` stays countable for source quality KPIs.
