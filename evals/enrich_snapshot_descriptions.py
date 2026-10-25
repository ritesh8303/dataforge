"""Backfill COMPLETE job descriptions into the frozen final snapshot.

Fetches original posting HTML (and known ATS JSON APIs when possible) and
stores the full plain-text description — no 300/4000 board-style truncation.
"""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html import unescape
from pathlib import Path

from lxml import html as lhtml

FINAL = Path(__file__).resolve().parent / "data" / "final"
SNAPSHOT = FINAL / "jobs_snapshot.json"
BACKUP = FINAL / "jobs_snapshot_desc300_backup.json"
META = FINAL / "jobs_snapshot_enrich_meta.json"
# Soft safety only (pathological pages); normal JDs are far smaller.
MAX_CHARS = 80_000
SLEEP_S = 0.4
UA = "DataForgeThesisBot/1.1 (+thesis second-rater; full JD backfill)"

_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")

SELECTORS = [
    "//*[contains(@class,'job-description')]",
    "//*[contains(@class,'job_description')]",
    "//*[contains(@class,'jobDescription')]",
    "//*[contains(@class,'job--description')]",
    "//*[@id='content']",
    "//*[@id='job-description']",
    "//*[@data-qa='job-description']",
    "//*[contains(@class,'posting-page')]",
    "//*[contains(@class,'posting')]//*[contains(@class,'content')]",
    "//article",
    "//main",
    "//*[contains(@class,'description')]",
]


def plain_from_html(raw: str) -> str:
    try:
        doc = lhtml.fromstring(raw)
    except Exception:
        text = _TAG_RE.sub(" ", unescape(raw or ""))
        return _SPACE_RE.sub(" ", text).strip()[:MAX_CHARS]

    for bad in doc.xpath("//script|//style|//noscript|//svg|//nav|//footer|//header|//iframe|//form"):
        try:
            bad.drop_tree()
        except Exception:
            pass

    best = ""
    for xp in SELECTORS:
        try:
            nodes = doc.xpath(xp)
        except Exception:
            nodes = []
        for node in nodes[:5]:
            text = _SPACE_RE.sub(" ", " ".join(node.text_content().split())).strip()
            if len(text) > len(best):
                best = text

    if len(best) < 400 and doc.body is not None:
        best = _SPACE_RE.sub(" ", " ".join(doc.body.text_content().split())).strip()

    for noise in (
        "Skip to main content",
        "Cookie Settings",
        "Accept all cookies",
        "Privacy Policy",
        "Manage cookies",
        "All rights reserved",
    ):
        best = best.replace(noise, " ")
    return _SPACE_RE.sub(" ", best).strip()[:MAX_CHARS]


def fetch_bytes(url: str, accept: str = "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8") -> tuple[bytes | None, str, str]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": accept,
            "Accept-Language": "en-US,en;q=0.9,de;q=0.7",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=40) as resp:
            return resp.read(), "ok", resp.headers.get_content_charset() or "utf-8"
    except urllib.error.HTTPError as e:
        return None, f"http_{e.code}", "utf-8"
    except Exception as e:
        return None, f"err_{type(e).__name__}", "utf-8"


def fetch_html(url: str) -> tuple[str | None, str]:
    raw, status, charset = fetch_bytes(url)
    if raw is None:
        return None, status
    return raw.decode(charset, errors="replace"), status


def greenhouse_api_text(url: str) -> str | None:
    """Try Greenhouse boards API for full HTML content."""
    # https://job-boards.greenhouse.io/{board}/jobs/{id}
    # https://boards.greenhouse.io/{board}/jobs/{id}
    m = re.search(r"greenhouse\.io/([^/]+)/jobs/(\d+)", url, re.I)
    if not m:
        # databricks style ?gh_jid=
        m2 = re.search(r"[?&]gh_jid=(\d+)", url, re.I)
        if not m2:
            return None
        jid = m2.group(1)
        # unknown board — cannot call API without board token
        return None
    board, jid = m.group(1), m.group(2)
    api = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs/{jid}?questions=false"
    raw, status, charset = fetch_bytes(api, accept="application/json")
    if raw is None:
        return None
    try:
        data = json.loads(raw.decode(charset, errors="replace"))
    except Exception:
        return None
    content = data.get("content") or data.get("abs_content") or ""
    if not content:
        return None
    return plain_from_html(f"<div>{content}</div>")


def lever_api_text(url: str) -> str | None:
    # https://jobs.lever.co/{company}/{uuid}
    m = re.search(r"jobs\.lever\.co/([^/]+)/([0-9a-f-]{36})", url, re.I)
    if not m:
        return None
    company, uid = m.group(1), m.group(2)
    api = f"https://api.lever.co/v0/postings/{company}/{uid}?mode=json"
    raw, status, charset = fetch_bytes(api, accept="application/json")
    if raw is None:
        return None
    try:
        data = json.loads(raw.decode(charset, errors="replace"))
    except Exception:
        return None
    parts = []
    for key in ("descriptionPlain", "description", "lists"):
        val = data.get(key)
        if isinstance(val, str) and val.strip():
            parts.append(val if key.endswith("Plain") else plain_from_html(val))
        elif key == "lists" and isinstance(val, list):
            for item in val:
                if not isinstance(item, dict):
                    continue
                t = item.get("text") or ""
                content = item.get("content") or ""
                if t:
                    parts.append(str(t))
                if content:
                    parts.append(plain_from_html(content) if "<" in str(content) else str(content))
    text = _SPACE_RE.sub(" ", "\n\n".join(p for p in parts if p)).strip()
    return text[:MAX_CHARS] if text else None


def ashby_api_text(url: str) -> str | None:
    # https://jobs.ashbyhq.com/{org}/{id}
    m = re.search(r"jobs\.ashbyhq\.com/([^/]+)/([0-9a-f-]{8,})", url, re.I)
    if not m:
        return None
    # Ashby public job board pages are JS-heavy; try opengraph scrape via HTML first.
    return None


def extract_full(url: str) -> tuple[str | None, str]:
    # Prefer structured ATS APIs when available (complete content).
    for fn, label in (
        (greenhouse_api_text, "greenhouse_api"),
        (lever_api_text, "lever_api"),
    ):
        try:
            text = fn(url)
        except Exception:
            text = None
        if text and len(text) >= 400:
            return text, label
        time.sleep(0.15)

    html, status = fetch_html(url)
    if html:
        text = plain_from_html(html)
        if text and len(text) >= 200:
            return text, f"html_{status}"
    # Dead/blocked pages: try Wayback Machine snapshot.
    wb = wayback_latest(url)
    if wb:
        time.sleep(0.2)
        html2, status2 = fetch_html(wb)
        if html2:
            text2 = plain_from_html(html2)
            if text2 and len(text2) >= 200:
                return text2, f"wayback_{status2}"
    if html:
        text = plain_from_html(html)
        return text or None, f"thin_{status}"
    return None, status


def wayback_latest(url: str) -> str | None:
    api = "https://archive.org/wayback/available?url=" + urllib.parse.quote(url, safe="")
    raw, status, charset = fetch_bytes(api, accept="application/json")
    if raw is None:
        return None
    try:
        data = json.loads(raw.decode(charset, errors="replace"))
    except Exception:
        return None
    closest = (data.get("archived_snapshots") or {}).get("closest") or {}
    if not closest.get("available"):
        return None
    return closest.get("url")


def needs_refresh(job: dict) -> bool:
    desc = job.get("description") or ""
    src = job.get("description_source") or ""
    # Greenhouse/Lever API payloads are complete — keep them.
    if src == "ats_api_backfill" and len(desc) >= 1200:
        return False
    # Long HTML scrapes are usually complete.
    if src == "job_url_backfill" and len(desc) >= 7000:
        return False
    return True


def main() -> int:
    jobs = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    if not BACKUP.exists():
        BACKUP.write_text(json.dumps(jobs, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    stats = {
        "started": datetime.now(tz=timezone.utc).isoformat(),
        "n": len(jobs),
        "refreshed": 0,
        "skipped_ok": 0,
        "failed": 0,
        "failures": [],
        "length_before_avg": 0.0,
        "length_after_avg": 0.0,
        "max_after": 0,
        "ge_8k": 0,
        "sources": {},
    }
    before = [len(j.get("description") or "") for j in jobs]
    stats["length_before_avg"] = round(sum(before) / max(1, len(before)), 1)

    for i, job in enumerate(jobs, 1):
        url = (job.get("job_url") or "").strip()
        old = job.get("description") or ""
        if not url:
            stats["failed"] += 1
            print(f"[{i}/{len(jobs)}] NO URL {job.get('job_id')}")
            continue
        if not needs_refresh(job):
            stats["skipped_ok"] += 1
            src = job.get("description_source") or "existing"
            stats["sources"][src] = stats["sources"].get(src, 0) + 1
            print(f"[{i}/{len(jobs)}] SKIP ok len={len(old)} {job.get('title','')[:40]}")
            continue

        text, status = extract_full(url)
        time.sleep(SLEEP_S)
        if not text or len(text) < 200:
            stats["failed"] += 1
            if len(stats["failures"]) < 40:
                stats["failures"].append(
                    {"job_id": job.get("job_id"), "status": status, "url": url, "got": len(text or "")}
                )
            print(f"[{i}/{len(jobs)}] FAIL {status} got={len(text or '')} {job.get('job_id')}")
            continue

        # Always take longer complete text; also replace exact 4000 caps.
        if len(text) >= len(old) or len(old) >= 3990 or len(old) < 900:
            job["description"] = text
            job["description_source"] = (
                "ats_api_backfill" if status.endswith("_api") or status in {"greenhouse_api", "lever_api"} else "job_url_backfill"
            )
            job["description_chars"] = len(text)
            stats["refreshed"] += 1
            stats["sources"][job["description_source"]] = stats["sources"].get(job["description_source"], 0) + 1
            print(f"[{i}/{len(jobs)}] OK {len(old)}→{len(text)} via {status} · {job.get('title','')[:42]}")
        else:
            stats["skipped_ok"] += 1
            print(f"[{i}/{len(jobs)}] KEEP existing longer {len(old)}>{len(text)}")

    after = [len(j.get("description") or "") for j in jobs]
    stats["length_after_avg"] = round(sum(after) / max(1, len(after)), 1)
    stats["max_after"] = max(after) if after else 0
    stats["ge_8k"] = sum(1 for x in after if x >= 8000)
    stats["exactly_4000"] = sum(1 for x in after if x == 4000)
    stats["finished"] = datetime.now(tz=timezone.utc).isoformat()

    SNAPSHOT.write_text(json.dumps(jobs, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    META.write_text(json.dumps(stats, indent=2), encoding="utf-8")
    print(json.dumps({k: stats[k] for k in (
        "n", "refreshed", "skipped_ok", "failed", "length_before_avg",
        "length_after_avg", "max_after", "ge_8k", "exactly_4000", "sources"
    )}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
