"""Saved-search alerts: filter Gold board, track seen IDs, webhook / SES email."""

from __future__ import annotations

import csv
import json
import os
import smtplib
import urllib.request
from dataclasses import asdict, dataclass, field
from email.message import EmailMessage
from io import StringIO
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SEEN = ROOT / "data" / "alert_seen_job_ids.json"
DEFAULT_SEARCHES = ROOT / "config" / "saved_searches.json"
LOCAL_GOLD = ROOT / "data" / "gold" / "all_jobs.csv"


@dataclass
class SavedSearch:
    name: str = "default"
    field: str = ""
    city: str = ""
    employment: str = ""
    keywords: str = ""
    webhook_url: str = ""
    email_to: str = ""


def load_jobs() -> list[dict]:
    bucket = os.environ.get("GOLD_BUCKET", "").strip()
    if bucket:
        import boto3

        obj = boto3.client("s3").get_object(Bucket=bucket, Key="all_jobs.csv")
        return list(csv.DictReader(StringIO(obj["Body"].read().decode("utf-8-sig"))))
    if LOCAL_GOLD.exists():
        with LOCAL_GOLD.open(encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    raise FileNotFoundError("No local data/gold/all_jobs.csv and GOLD_BUCKET unset")


def load_searches(path: str | Path | None = None) -> list[SavedSearch]:
    p = Path(path or os.environ.get("SAVED_SEARCHES_PATH") or DEFAULT_SEARCHES)
    if not p.exists():
        return []
    raw = json.loads(p.read_text(encoding="utf-8"))
    rows = raw if isinstance(raw, list) else raw.get("searches") or []
    out = []
    for r in rows:
        if isinstance(r, dict):
            out.append(SavedSearch(**{k: r.get(k, "") for k in SavedSearch.__dataclass_fields__}))
    return out


def match_job(job: dict, search: SavedSearch) -> bool:
    if search.field:
        f = (job.get("ai_field") or job.get("field") or "").lower()
        title = (job.get("title") or "").lower()
        if search.field.lower() not in f and search.field.lower().replace("_", " ") not in title:
            return False
    if search.city and search.city.lower() not in (job.get("location") or "").lower():
        return False
    if search.employment:
        emp = (job.get("employment_type") or job.get("ai_seniority") or "").lower()
        if search.employment.lower() not in emp:
            return False
    if search.keywords:
        blob = " ".join(
            [
                str(job.get("title") or ""),
                str(job.get("tags") or ""),
                str(job.get("canonical_title_en") or ""),
                str(job.get("description") or "")[:500],
            ]
        ).lower()
        for kw in search.keywords.lower().split():
            if kw and kw not in blob:
                return False
    return True


def load_seen(path: Path | None = None) -> set[str]:
    p = path or DEFAULT_SEEN
    if not p.exists():
        return set()
    return set(json.loads(p.read_text(encoding="utf-8")).get("job_ids") or [])


def save_seen(ids: set[str], path: Path | None = None) -> None:
    p = path or DEFAULT_SEEN
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"job_ids": sorted(ids)}, indent=2), encoding="utf-8")


def post_webhook(url: str, payload: dict[str, Any]) -> None:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=30)


def send_email_smtp(to_addr: str, subject: str, body: str) -> None:
    """Send via SMTP (ALERT_SMTP_* env) or skip if unset."""
    host = os.environ.get("ALERT_SMTP_HOST", "").strip()
    if not host:
        # Optional AWS SES via boto3
        if os.environ.get("ALERT_SES_FROM"):
            import boto3

            boto3.client("ses", region_name=os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")).send_email(
                Source=os.environ["ALERT_SES_FROM"],
                Destination={"ToAddresses": [to_addr]},
                Message={
                    "Subject": {"Data": subject},
                    "Body": {"Text": {"Data": body}},
                },
            )
            return
        raise RuntimeError("No ALERT_SMTP_HOST or ALERT_SES_FROM configured")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("ALERT_SMTP_FROM", "dataforge@localhost")
    msg["To"] = to_addr
    msg.set_content(body)
    port = int(os.environ.get("ALERT_SMTP_PORT", "587"))
    user = os.environ.get("ALERT_SMTP_USER", "")
    password = os.environ.get("ALERT_SMTP_PASSWORD", "")
    with smtplib.SMTP(host, port, timeout=30) as smtp:
        smtp.starttls()
        if user:
            smtp.login(user, password)
        smtp.send_message(msg)


def run_alerts(
    *,
    searches: list[SavedSearch] | None = None,
    dry_run: bool = False,
    webhook_fallback: str = "",
) -> dict[str, Any]:
    jobs = load_jobs()
    seen = load_seen()
    searches = searches if searches is not None else load_searches()
    if not searches:
        searches = [SavedSearch(name="cli")]

    results: list[dict[str, Any]] = []
    new_ids: set[str] = set()
    for search in searches:
        fresh = []
        for job in jobs:
            jid = str(job.get("job_id") or "")
            if not jid or jid in seen:
                continue
            if match_job(job, search):
                fresh.append(job)
                new_ids.add(jid)
        payload = {
            "search": asdict(search),
            "count": len(fresh),
            "jobs": [
                {
                    "title": j.get("title"),
                    "company": j.get("company"),
                    "location": j.get("location"),
                    "url": j.get("job_url") or j.get("url"),
                    "job_id": j.get("job_id"),
                }
                for j in fresh[:20]
            ],
        }
        results.append(payload)
        if dry_run or not fresh:
            continue
        hook = search.webhook_url or webhook_fallback or os.environ.get("ALERT_WEBHOOK_URL", "")
        if hook:
            post_webhook(hook, {"text": f"DataForge [{search.name}]: {len(fresh)} new jobs", **payload})
        if search.email_to:
            lines = [f"{j['title']} — {j['company']} ({j['location']})\n{j['url']}" for j in payload["jobs"]]
            send_email_smtp(
                search.email_to,
                f"DataForge alert: {len(fresh)} new jobs ({search.name})",
                "\n\n".join(lines) or "No jobs",
            )

    if not dry_run:
        all_ids = {str(j.get("job_id")) for j in jobs if j.get("job_id")} | seen | new_ids
        save_seen(all_ids)

    return {
        "scanned": len(jobs),
        "seen": len(seen),
        "searches": len(searches),
        "results": results,
        "new_total": sum(r["count"] for r in results),
    }
