"""Backfill fuller job descriptions into the frozen final snapshot.

Root cause: gold board truncated descriptions to 300 chars (company intro).
This fetches the original posting HTML for each snapshot job_url and writes
up to 4000 chars of plain text back into jobs_snapshot.json (same job_ids).
"""

from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from html import unescape
from pathlib import Path

from lxml import html as lhtml

FINAL = Path(__file__).resolve().parent / "data" / "final"
SNAPSHOT = FINAL / "jobs_snapshot.json"
BACKUP = FINAL / "jobs_snapshot_desc300_backup.json"
META = FINAL / "jobs_snapshot_enrich_meta.json"
MAX_CHARS = 4000
SLEEP_S = 0.35
UA = "DataForgeThesisBot/1.0 (+second-rater description backfill; research use)"

_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")

SELECTORS = [
    "//*[contains(@class,'job-description')]",
    "//*[contains(@class,'job_description')]",
    "//*[contains(@class,'jobDescription')]",
    "//*[@id='content']",
    "//*[contains(@class,'content-intro')]/ancestor::*[contains(@class,'content') or self::article][1]",
    "//article",
    "//main",
    "//*[contains(@class,'description')]",
    "//*[@data-qa='job-description']",
    "//*[contains(@class,'posting-page')]",
]


def plain_from_html(raw: str) -> str:
    try:
        doc = lhtml.fromstring(raw)
    except Exception:
        text = _TAG_RE.sub(" ", unescape(raw or ""))
        return _SPACE_RE.sub(" ", text).strip()

    for bad in doc.xpath("//script|//style|//noscript|//svg|//nav|//footer|//header|//iframe"):
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
        for node in nodes[:3]:
            text = _SPACE_RE.sub(" ", " ".join(node.text_content().split())).strip()
            if len(text) > len(best):
                best = text
        if len(best) >= 600:
            break

    if len(best) < 200 and doc.body is not None:
        best = _SPACE_RE.sub(" ", " ".join(doc.body.text_content().split())).strip()

    # Drop very common chrome leftovers
    for noise in (
        "Skip to main content",
        "Cookie Settings",
        "Accept all cookies",
        "Privacy Policy",
    ):
        best = best.replace(noise, " ")
    best = _SPACE_RE.sub(" ", best).strip()
    return best[:MAX_CHARS]


def fetch(url: str) -> tuple[str | None, str]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.8,de;q=0.7",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=35) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            raw = resp.read().decode(charset, errors="replace")
            return raw, "ok"
    except urllib.error.HTTPError as e:
        return None, f"http_{e.code}"
    except Exception as e:
        return None, f"err_{type(e).__name__}"


def main() -> int:
    jobs = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    if not BACKUP.exists():
        BACKUP.write_text(json.dumps(jobs, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    stats = {
        "started": datetime.now(tz=timezone.utc).isoformat(),
        "n": len(jobs),
        "ok": 0,
        "kept_short": 0,
        "failed": 0,
        "longer": 0,
        "failures": [],
        "length_before_avg": 0,
        "length_after_avg": 0,
    }
    before_lens = [len(j.get("description") or "") for j in jobs]
    stats["length_before_avg"] = round(sum(before_lens) / max(1, len(before_lens)), 1)

    for i, job in enumerate(jobs, 1):
        url = (job.get("job_url") or "").strip()
        old = job.get("description") or ""
        if not url:
            stats["kept_short"] += 1
            continue
        html, status = fetch(url)
        time.sleep(SLEEP_S)
        if not html:
            stats["failed"] += 1
            if len(stats["failures"]) < 25:
                stats["failures"].append({"job_id": job.get("job_id"), "status": status, "url": url})
            print(f"[{i}/{len(jobs)}] FAIL {status} {job.get('job_id')}")
            continue
        text = plain_from_html(html)
        # Prefer fetched text only if clearly richer than the 300-char board snippet
        if len(text) >= max(400, len(_TAG_RE.sub(" ", old)) + 80):
            # Keep a light HTML wrapper-free plain description for ranking/raters
            job["description"] = text
            job["description_source"] = "job_url_backfill"
            stats["ok"] += 1
            stats["longer"] += 1
            print(f"[{i}/{len(jobs)}] OK {len(old)}→{len(text)} {job.get('title','')[:48]}")
        else:
            stats["kept_short"] += 1
            print(f"[{i}/{len(jobs)}] KEEP short ({len(text)} chars) {job.get('job_id')}")

    after_lens = [len(j.get("description") or "") for j in jobs]
    stats["length_after_avg"] = round(sum(after_lens) / max(1, len(after_lens)), 1)
    stats["finished"] = datetime.now(tz=timezone.utc).isoformat()
    stats["max_after"] = max(after_lens) if after_lens else 0

    SNAPSHOT.write_text(json.dumps(jobs, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    META.write_text(json.dumps(stats, indent=2), encoding="utf-8")
    print(json.dumps({k: stats[k] for k in ("n", "ok", "longer", "kept_short", "failed", "length_before_avg", "length_after_avg", "max_after")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
