"""Probe apply URLs and write link_health.csv for Gold trust merge.

Usage (from repo root):
  py -3 scripts/check_apply_links.py --limit 200
  py -3 scripts/check_apply_links.py --csv data/gold/all_jobs.csv --out data/gold/link_health.csv

Does not invent jobs — only records HTTP reachability of existing apply links.
Dead links are demoted via trust_tier=dead on the next Gold attach_trust_signals pass.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from processing.trust_signals import preferred_apply_url  # noqa: E402

USER_AGENT = "DataForgeLinkHealth/1.0 (+https://ritesh8303.github.io/dataforge/; polite daily probe)"


def _probe(url: str, timeout: float = 8.0) -> dict[str, Any]:
    if not url or url == "#":
        return {"status": "missing", "http_status": "", "final_url": "", "error": "empty"}
    req = Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=timeout) as resp:  # noqa: S310 — intentional URL probe
            code = getattr(resp, "status", None) or resp.getcode()
            final = resp.geturl() or url
            if int(code) >= 400:
                return {"status": "dead", "http_status": code, "final_url": final, "error": ""}
            return {"status": "alive", "http_status": code, "final_url": final, "error": ""}
    except HTTPError as exc:
        # Some ATS reject HEAD — retry GET lightly
        if exc.code in {401, 403, 405}:
            try:
                get_req = Request(url, method="GET", headers={"User-Agent": USER_AGENT})
                with urlopen(get_req, timeout=timeout) as resp:  # noqa: S310
                    code = getattr(resp, "status", None) or resp.getcode()
                    return {
                        "status": "alive" if int(code) < 400 else "dead",
                        "http_status": code,
                        "final_url": resp.geturl() or url,
                        "error": f"head_{exc.code}",
                    }
            except Exception as inner:  # noqa: BLE001
                return {
                    "status": "dead" if exc.code in {404, 410} else "unknown",
                    "http_status": exc.code,
                    "final_url": url,
                    "error": str(inner)[:120],
                }
        status = "dead" if exc.code in {404, 410, 451} else "unknown"
        return {"status": status, "http_status": exc.code, "final_url": url, "error": str(exc)[:120]}
    except (URLError, TimeoutError, OSError) as exc:
        return {"status": "unknown", "http_status": "", "final_url": url, "error": str(exc)[:120]}


def load_jobs(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def main() -> int:
    parser = argparse.ArgumentParser(description="Check apply link health for Gold jobs")
    parser.add_argument("--csv", type=Path, default=ROOT / "data" / "gold" / "all_jobs.csv")
    parser.add_argument("--out", type=Path, default=ROOT / "data" / "gold" / "link_health.csv")
    parser.add_argument("--limit", type=int, default=300, help="Max jobs to probe this run")
    parser.add_argument("--delay", type=float, default=0.25, help="Seconds between requests")
    parser.add_argument("--product-only", action="store_true", help="Only audience_accept rows")
    args = parser.parse_args()

    if not args.csv.exists():
        print(f"Missing {args.csv} — download Gold or point --csv at all_jobs.csv")
        return 1

    jobs = load_jobs(args.csv)
    if args.product_only:
        jobs = [
            j
            for j in jobs
            if str(j.get("audience_accept") or "").lower() in {"1", "true", "yes", "y"}
        ]
    jobs = jobs[: max(0, args.limit)]
    checked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []
    for i, job in enumerate(jobs, 1):
        url = preferred_apply_url(job)
        result = _probe(url)
        rows.append(
            {
                "job_id": job.get("job_id") or "",
                "url": url,
                "status": result["status"],
                "link_status": result["status"],
                "http_status": result["http_status"],
                "final_url": result["final_url"],
                "checked_at": checked_at,
                "error": result["error"],
            }
        )
        if i % 25 == 0:
            print(f"Probed {i}/{len(jobs)}…")
        time.sleep(max(0.0, args.delay))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "job_id",
        "url",
        "status",
        "link_status",
        "http_status",
        "final_url",
        "checked_at",
        "error",
    ]
    with args.out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    counts: dict[str, int] = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(f"Wrote {args.out} ({len(rows)} rows) · {counts}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
