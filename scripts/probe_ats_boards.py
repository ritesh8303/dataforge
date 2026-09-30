"""Probe ATS board tokens / Personio XML hosts; print OK vs BAD."""
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

HEADERS = {
    "User-Agent": "DataForge Job Aggregator/1.0 (+public career feed ingestion)",
    "Accept": "application/json, text/plain, */*",
}
ROOT = Path(__file__).resolve().parents[1]


def probe_ats(row: dict) -> tuple:
    url = row.get("careers_url") or ""
    company = row.get("company")
    ats_hint = row.get("ats")
    try:
        if "greenhouse.io" in url:
            slug = url.rstrip("/").split("/")[-1]
            r = requests.get(
                f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
                headers=HEADERS,
                params={"content": "false"},
                timeout=12,
                allow_redirects=False,
            )
            n = len(r.json().get("jobs", [])) if r.status_code == 200 else 0
            return ("greenhouse", company, slug, r.status_code, n, url)
        if "ashbyhq.com" in url:
            slug = url.rstrip("/").split("/")[-1]
            r = requests.get(
                f"https://api.ashbyhq.com/posting-api/job-board/{slug}",
                headers=HEADERS,
                timeout=12,
                allow_redirects=False,
            )
            n = 0
            if r.status_code == 200:
                data = r.json()
                n = len(data.get("jobs", [])) if isinstance(data, dict) else 0
            return ("ashby", company, slug, r.status_code, n, url)
        if "smartrecruiters.com" in url:
            slug = url.rstrip("/").split("/")[-1]
            r = requests.get(
                f"https://api.smartrecruiters.com/v1/companies/{slug}/postings",
                headers=HEADERS,
                params={"limit": 1},
                timeout=12,
                allow_redirects=False,
            )
            return ("smartrecruiters", company, slug, r.status_code, 1 if r.status_code == 200 else 0, url)
        if "recruitee.com" in url:
            slug = url.split("//", 1)[1].split(".", 1)[0]
            r = requests.get(
                f"https://{slug}.recruitee.com/api/offers/",
                headers=HEADERS,
                timeout=12,
                allow_redirects=False,
            )
            n = len(r.json().get("offers", [])) if r.status_code == 200 else 0
            return ("recruitee", company, slug, r.status_code, n, url)
        if ats_hint == "workable" or "workable" in url:
            slug = row.get("slug") or url.rstrip("/").split("/")[-1]
            r = requests.get(
                f"https://apply.workable.com/api/v3/accounts/{slug}/jobs",
                headers=HEADERS,
                timeout=12,
                allow_redirects=False,
            )
            return ("workable", company, slug, r.status_code, 1 if r.status_code in (200, 201) else 0, url)
        return ("other", company, "", 0, 0, url)
    except Exception as exc:  # noqa: BLE001
        return ("err", company, "", type(exc).__name__, 0, url)


def probe_personio(row: dict) -> tuple:
    slug = row["slug"]
    tld = str(row.get("tld") or "de").strip().lstrip(".")
    hosts = [
        f"https://{slug}.jobs.personio.{tld}/xml",
        f"https://{slug}.jobs.personio.{'com' if tld == 'de' else 'de'}/xml",
    ]
    for host in hosts:
        try:
            r = requests.get(
                host,
                headers=HEADERS,
                params={"language": "en"},
                timeout=12,
                allow_redirects=False,
            )
            if r.status_code in (301, 302, 303, 307, 308):
                loc = r.headers.get("Location", "")
                # Marketing redirect = dead tenant
                if "personio.com" in loc and f"{slug}." not in loc:
                    continue
                continue
            if r.status_code == 429:
                time.sleep(2.5)
                continue
            if r.status_code == 200 and ("<position" in r.text.lower() or "<job" in r.text.lower()):
                return (slug, host, r.status_code, r.text.lower().count("<position"))
        except Exception:  # noqa: BLE001
            continue
    return (slug, "", 0, 0)


def main() -> None:
    ats = json.loads((ROOT / "config/sources/dach_ats.json").read_text(encoding="utf-8"))
    personio = json.loads((ROOT / "config/sources/personio_tenants.json").read_text(encoding="utf-8"))

    ok, bad = [], []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(probe_ats, row) for row in ats]
        for fut in as_completed(futs):
            res = fut.result()
            if isinstance(res[3], int) and res[3] == 200:
                ok.append(res)
            else:
                bad.append(res)

    print(f"ATS OK {len(ok)} BAD {len(bad)}")
    for r in sorted(bad, key=lambda x: (str(x[0]), str(x[1]))):
        print("BAD", r)
    print("--- OK ---")
    for r in sorted(ok, key=lambda x: (str(x[0]), str(x[1]))):
        print("OK", r)

    p_ok, p_bad = [], []
    for row in personio:
        res = probe_personio(row)
        if res[2] == 200:
            p_ok.append(res)
        else:
            p_bad.append(res[0])
        time.sleep(0.4)
    print(f"PERSONIO OK {len(p_ok)} BAD {len(p_bad)}")
    print("BAD personio", p_bad)
    print("OK personio", [(x[0], x[3]) for x in p_ok])


if __name__ == "__main__":
    main()
