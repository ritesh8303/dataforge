#!/usr/bin/env python3
"""Generate static JobPosting SEO pages under docs/jobs/ from Gold CSV.

Usage:
  py -3 scripts/generate_seo_job_pages.py --limit 100
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from html import escape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "data" / "gold" / "all_jobs.csv"
OUT_DIR = ROOT / "docs" / "jobs"


def _slug(job: dict) -> str:
    jid = str(job.get("job_id") or "job")
    title = re.sub(r"[^a-z0-9]+", "-", str(job.get("title") or "role").lower()).strip("-")[:60]
    return f"{title}-{jid[-12:]}" if len(jid) > 12 else f"{title}-{jid}"


def _job_ld(job: dict, url: str) -> dict:
    salary_min = job.get("ai_salary_min") or job.get("salary")
    base: dict = {
        "@context": "https://schema.org",
        "@type": "JobPosting",
        "title": job.get("canonical_title_en") or job.get("title"),
        "description": (job.get("ai_summary") or job.get("description") or job.get("title") or "")[:5000],
        "datePosted": job.get("date_added") or job.get("published_at") or datetime.now(timezone.utc).date().isoformat(),
        "hiringOrganization": {
            "@type": "Organization",
            "name": job.get("company") or "Unknown",
        },
        "jobLocation": {
            "@type": "Place",
            "address": {
                "@type": "PostalAddress",
                "addressLocality": job.get("location") or "",
                "addressCountry": job.get("region") or "EU",
            },
        },
        "url": job.get("job_url") or job.get("url") or url,
        "identifier": job.get("job_id"),
        "employmentType": (job.get("employment_type") or job.get("ai_seniority") or "OTHER").upper(),
    }
    if salary_min not in (None, "", "nan"):
        try:
            base["baseSalary"] = {
                "@type": "MonetaryAmount",
                "currency": "EUR",
                "value": {
                    "@type": "QuantitativeValue",
                    "value": float(salary_min),
                    "unitText": str(job.get("ai_salary_unit") or "MONTH").replace("eur_", "").upper(),
                },
            }
        except (TypeError, ValueError):
            pass
    return base


def _page_html(job: dict, slug: str) -> str:
    title = escape(str(job.get("title") or "Job"))
    company = escape(str(job.get("company") or ""))
    location = escape(str(job.get("location") or ""))
    apply_url = escape(str(job.get("job_url") or job.get("url") or "#"))
    summary = escape(str(job.get("ai_summary") or (job.get("description") or "")[:400]))
    canon = escape(str(job.get("canonical_title_en") or ""))
    page_url = f"https://ritesh8303.github.io/dataforge/jobs/{slug}.html"
    ld = json.dumps(_job_ld(job, page_url), ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{title} — {company} | DataForge</title>
  <meta name="description" content="{summary[:160]}" />
  <link rel="stylesheet" href="../components/design-system.css" />
  <script type="application/ld+json">{ld}</script>
</head>
<body class="df-page">
  <main class="df-container" style="padding:2rem 1rem;max-width:720px">
    <p><a href="../jobs.html">← Job board</a></p>
    <h1>{title}</h1>
    <p class="df-text-secondary">{company} · {location}</p>
    {f'<p><strong>Canonical:</strong> {canon}</p>' if canon else ''}
    <p>{summary}</p>
    <p><a class="df-btn df-btn-primary" href="{apply_url}" rel="noopener noreferrer">Apply / source</a></p>
    <p class="df-text-muted" style="font-size:0.8rem">job_id: {escape(str(job.get("job_id") or ""))}</p>
  </main>
</body>
</html>
"""


def _is_product(job: dict) -> bool:
    v = str(job.get("audience_accept") or "").strip().lower()
    return v in {"1", "true", "yes"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=800)
    parser.add_argument("--csv", type=Path, default=GOLD)
    parser.add_argument(
        "--all-jobs",
        action="store_true",
        help="Include the full lakehouse CSV instead of audience_accept only",
    )
    args = parser.parse_args()
    if not args.csv.exists():
        print(f"missing {args.csv}")
        return 1
    with args.csv.open(encoding="utf-8-sig", newline="") as fh:
        jobs = list(csv.DictReader(fh))
    if not args.all_jobs:
        product = [j for j in jobs if _is_product(j)]
        jobs = product or jobs
    jobs = jobs[: max(0, args.limit)]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # Drop stale pages so the sitemap stays in sync with Gold.
    for stale in OUT_DIR.glob("*.html"):
        stale.unlink()
    index_rows = []
    for job in jobs:
        slug = _slug(job)
        path = OUT_DIR / f"{slug}.html"
        path.write_text(_page_html(job, slug), encoding="utf-8")
        index_rows.append(
            {
                "slug": slug,
                "title": job.get("title"),
                "company": job.get("company"),
                "job_id": job.get("job_id"),
            }
        )
    (OUT_DIR / "index.json").write_text(json.dumps(index_rows, indent=2), encoding="utf-8")
    urls = [f"https://ritesh8303.github.io/dataforge/jobs/{r['slug']}.html" for r in index_rows]
    (OUT_DIR / "sitemap-jobs.txt").write_text("\n".join(urls) + "\n", encoding="utf-8")
    sitemap = ROOT / "docs" / "sitemap.xml"
    static = [
        "https://ritesh8303.github.io/dataforge/",
        "https://ritesh8303.github.io/dataforge/jobs.html",
        "https://ritesh8303.github.io/dataforge/agent.html",
        "https://ritesh8303.github.io/dataforge/dashboard.html",
        "https://ritesh8303.github.io/dataforge/roi.html",
        "https://ritesh8303.github.io/dataforge/about.html",
    ]
    loc_xml = "\n".join(f"  <url><loc>{escape(u)}</loc></url>" for u in static + urls)
    sitemap.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{loc_xml}\n"
        "</urlset>\n",
        encoding="utf-8",
    )
    print(f"wrote {len(index_rows)} pages -> {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
