"""Replace snapshot jobs with unrecoverable short JDs by fully-fetchable live jobs.

Keeps N=120. Drops old job_ids from labels_author relevance_grades.
"""

from __future__ import annotations

import json
import random
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from enrich_snapshot_remainders import (
    MAX_CHARS,
    from_ashby,
    from_gh_jid,
    greenhouse_text,
    plain_html,
)
from enrich_snapshot_descriptions import extract_full

FINAL = Path(__file__).resolve().parent / "data" / "final"
SNAPSHOT = FINAL / "jobs_snapshot.json"
LABELS = FINAL / "labels_author.json"
META = FINAL / "jobs_snapshot_replace_meta.json"
API = "https://br1sodhn2e.execute-api.eu-central-1.amazonaws.com/"
MIN_OK = 1200
SEED = 20261025
N = 120


def fetch_live(limit: int = 800) -> list[dict]:
    jobs: list[dict] = []
    offset = 0
    while len(jobs) < limit:
        params = urllib.parse.urlencode(
            {
                "status": "active",
                "sort": "newest",
                "limit": 100,
                "offset": offset,
            }
        )
        url = f"{API}?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "DataForgeThesisBot/1.2"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        batch = data.get("jobs") or []
        if not batch:
            break
        jobs.extend(batch)
        offset += len(batch)
        if offset > 5000:
            break
    # dedupe
    seen = set()
    out = []
    for j in jobs:
        jid = str(j.get("job_id") or "")
        if not jid or jid in seen:
            continue
        seen.add(jid)
        out.append(j)
    return out


def enrich_candidate(job: dict) -> str | None:
    # Prefer ATS structured paths, then HTML.
    for fn in (from_ashby, from_gh_jid):
        text, status = fn(job)
        if text and len(text) >= MIN_OK:
            return text
        time.sleep(0.05)
    url = (job.get("job_url") or "").strip()
    if not url:
        return None
    text, status = extract_full(url)
    time.sleep(0.25)
    if text and len(text) >= MIN_OK:
        return text
    return None


def normalize(j: dict, description: str) -> dict:
    return {
        "job_id": str(j.get("job_id") or ""),
        "title": j.get("title") or "",
        "company": j.get("company") or "",
        "location": j.get("location") or "",
        "description": description[:MAX_CHARS],
        "tags": j.get("tags") or "",
        "source": j.get("source") or "",
        "is_remote": j.get("is_remote"),
        "job_url": j.get("job_url") or "",
        "job_types": j.get("job_types") or "",
        "description_source": "replacement_full_jd",
        "description_chars": len(description[:MAX_CHARS]),
    }


def main() -> int:
    snap = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    labels = json.loads(LABELS.read_text(encoding="utf-8"))
    short = [j for j in snap if len(j.get("description") or "") < MIN_OK]
    keep_ids = {j["job_id"] for j in snap if len(j.get("description") or "") >= MIN_OK}
    print(f"snapshot={len(snap)} short={len(short)} keep={len(keep_ids)}")

    live = fetch_live(900)
    print(f"live_pool={len(live)}")
    rng = random.Random(SEED)
    rng.shuffle(live)

    replacements: list[dict] = []
    tried = 0
    for cand in live:
        if len(replacements) >= len(short):
            break
        jid = str(cand.get("job_id") or "")
        if not jid or jid in keep_ids:
            continue
        # skip known hard hosts that failed before unless ATS ids present
        url = (cand.get("job_url") or "").lower()
        if "eures" in url or "himalayas.app" in url:
            continue
        tried += 1
        text = enrich_candidate(cand)
        if not text:
            print(f"  skip thin {jid[:48]}")
            continue
        row = normalize(cand, text)
        replacements.append(row)
        keep_ids.add(jid)
        print(f"  + {len(text)} {row['title'][:50]}")

    if len(replacements) < len(short):
        print(f"WARN only {len(replacements)}/{len(short)} replacements found")

    removed_ids = {j["job_id"] for j in short}
    new_snap = [j for j in snap if j["job_id"] not in removed_ids]
    new_snap.extend(replacements[: len(short)])
    # pad if needed by keeping some short ones
    if len(new_snap) < N:
        new_snap.extend(short[: N - len(new_snap)])
    new_snap = new_snap[:N]
    rng.shuffle(new_snap)

    # scrub labels
    scrubbed = 0
    for q in labels:
        grades = q.get("relevance_grades") or {}
        before = len(grades)
        q["relevance_grades"] = {k: v for k, v in grades.items() if k not in removed_ids}
        scrubbed += before - len(q["relevance_grades"])

    SNAPSHOT.write_text(json.dumps(new_snap, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    LABELS.write_text(json.dumps(labels, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    meta = {
        "finished": datetime.now(tz=timezone.utc).isoformat(),
        "removed": sorted(removed_ids),
        "added": [j["job_id"] for j in replacements[: len(short)]],
        "n": len(new_snap),
        "short_after": sum(1 for j in new_snap if len(j.get("description") or "") < MIN_OK),
        "avg_chars": round(sum(len(j.get("description") or "") for j in new_snap) / len(new_snap), 1),
        "label_grades_removed": scrubbed,
        "tried_candidates": tried,
    }
    META.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(json.dumps({k: meta[k] for k in ("n", "short_after", "avg_chars", "label_grades_removed", "tried_candidates")}, indent=2))
    print("added", len(meta["added"]), "removed", len(meta["removed"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
