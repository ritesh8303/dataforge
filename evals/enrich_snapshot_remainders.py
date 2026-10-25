"""Fill the remaining short snapshot JDs via Greenhouse gh_jid + Ashby board API."""

from __future__ import annotations

import json
import re
import ssl
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
META = FINAL / "jobs_snapshot_remainder_meta.json"
MIN_OK = 1200
MAX_CHARS = 80_000
UA = "DataForgeThesisBot/1.2 (+remainder JD backfill)"

_SPACE = re.compile(r"\s+")
SSL_CTX = ssl.create_default_context()
try:
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE
except Exception:
    pass


def get(url: str, accept: str = "*/*") -> tuple[str | None, str]:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": UA, "Accept": accept, "Accept-Language": "en,de;q=0.8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=40, context=SSL_CTX) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.read().decode(charset, errors="replace"), "ok"
    except urllib.error.HTTPError as e:
        return None, f"http_{e.code}"
    except Exception as e:
        return None, f"err_{type(e).__name__}"


def plain_html(raw: str) -> str:
    try:
        doc = lhtml.fromstring(raw)
    except Exception:
        return _SPACE.sub(" ", unescape(re.sub(r"<[^>]+>", " ", raw or ""))).strip()[:MAX_CHARS]
    for bad in doc.xpath("//script|//style|//noscript|//svg|//nav|//footer|//header"):
        try:
            bad.drop_tree()
        except Exception:
            pass
    return _SPACE.sub(" ", " ".join(doc.text_content().split())).strip()[:MAX_CHARS]


def greenhouse_text(board: str, jid: str) -> str | None:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs/{jid}?questions=false"
    raw, status = get(url, "application/json")
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        return None
    content = data.get("content") or ""
    if not content:
        return None
    return plain_html(f"<div>{content}</div>")


def board_candidates(job: dict) -> list[str]:
    url = (job.get("job_url") or "").lower()
    company = re.sub(r"[^a-z0-9]+", "", (job.get("company") or "").lower())
    out: list[str] = []
    # domain hints
    for host, board in (
        ("fivetran.com", "fivetran"),
        ("mongodb.com", "mongodb"),
        ("elastic.co", "elastic"),
        ("hellofresh.com", "hellofresh"),
        ("databricks.com", "databricks"),
        ("stripe.com", "stripe"),
        ("affirm.com", "affirm"),
        ("vercel.com", "vercel"),
        ("reddit.com", "reddit"),
        ("twilio.com", "twilio"),
        ("n26.com", "n26"),
        ("raisin.com", "raisin"),
        ("commercetools", "commercetools"),
        ("sumup.com", "sumup"),
        ("getyourguide", "getyourguide"),
        ("grafana", "grafanalabs"),
    ):
        if host in url:
            out.append(board)
    if company:
        out.append(company)
    # greenhouse path boards
    m = re.search(r"greenhouse\.io/([^/]+)/jobs/", url)
    if m:
        out.insert(0, m.group(1))
    # uniq
    seen = set()
    uniq = []
    for b in out:
        if b and b not in seen:
            seen.add(b)
            uniq.append(b)
    return uniq


def from_gh_jid(job: dict) -> tuple[str | None, str]:
    url = job.get("job_url") or ""
    m = re.search(r"(?:gh_jid=|/jobs/)(\d{5,})", url)
    if not m:
        return None, "no_gh_jid"
    jid = m.group(1)
    for board in board_candidates(job):
        text = greenhouse_text(board, jid)
        time.sleep(0.15)
        if text and len(text) >= 400:
            return text, f"greenhouse:{board}:{jid}"
    return None, f"gh_miss:{jid}"


_ASHBY_CACHE: dict[str, list[dict]] = {}


def ashby_jobs(org: str) -> list[dict]:
    if org in _ASHBY_CACHE:
        return _ASHBY_CACHE[org]
    raw, status = get(
        f"https://api.ashbyhq.com/posting-api/job-board/{org}?includeCompensation=true",
        "application/json",
    )
    jobs = []
    if raw:
        try:
            jobs = json.loads(raw).get("jobs") or []
        except Exception:
            jobs = []
    _ASHBY_CACHE[org] = jobs
    return jobs


def from_ashby(job: dict) -> tuple[str | None, str]:
    url = job.get("job_url") or ""
    m = re.search(r"jobs\.ashbyhq\.com/([^/]+)/([0-9a-f-]{8,})", url, re.I)
    if not m:
        return None, "no_ashby"
    org, uid = m.group(1), m.group(2)
    for j in ashby_jobs(org):
        if str(j.get("id")) == uid:
            text = (j.get("descriptionPlain") or "").strip()
            if not text and j.get("descriptionHtml"):
                text = plain_html(j["descriptionHtml"])
            if text:
                return text[:MAX_CHARS], f"ashby:{org}:{uid}"
    return None, f"ashby_miss:{org}:{uid}"


def from_eures(job: dict) -> tuple[str | None, str]:
    url = job.get("job_url") or ""
    if "eures" not in url.lower() and "europa.eu" not in url.lower():
        return None, "no_eures"
    m = re.search(r"jv-details/([^?/]+)", url)
    if not m:
        return None, "no_eures_token"
    tok = m.group(1)
    # Official searchengine endpoints (SSL often needs relaxed context).
    candidates = [
        f"https://europa.eu/eures/eures-apps/searchengine/page/jv/id/{tok}",
        f"https://ec.europa.eu/eures/eures-apps/searchengine/page/jv/id/{tok}",
        f"https://europa.eu/eures/eures-apps/searchengine/page/jv/{tok}",
    ]
    for api in candidates:
        raw, status = get(api, "application/json,text/html")
        time.sleep(0.2)
        if not raw:
            continue
        text = ""
        if raw.lstrip().startswith("{") or raw.lstrip().startswith("["):
            try:
                data = json.loads(raw)
            except Exception:
                data = None
            if isinstance(data, dict):
                for key in (
                    "jobDescription",
                    "description",
                    "jvProfiles",
                    "positionOpening",
                ):
                    val = data.get(key)
                    if isinstance(val, str) and len(val) > 200:
                        text = plain_html(val) if "<" in val else val
                        break
                    if isinstance(val, list) and val:
                        blob = json.dumps(val)
                        text = plain_html(blob) if "<" in blob else _SPACE.sub(" ", blob)
                if not text:
                    # deep string harvest
                    blob = json.dumps(data)
                    # prefer long prose fields
                    strings = re.findall(r"\"((?:[^\"\\]|\\.){400,})\"", blob)
                    if strings:
                        text = max((unescape(s.encode().decode("unicode_escape", "ignore")) for s in strings), key=len)
                        text = plain_html(text) if "<" in text else _SPACE.sub(" ", text)
        else:
            text = plain_html(raw)
        if text and len(text) >= 400:
            return text[:MAX_CHARS], f"eures:{status}"
    return None, "eures_fail"


def from_himalayas(job: dict) -> tuple[str | None, str]:
    url = job.get("job_url") or ""
    if "himalayas.app" not in url:
        return None, "no_himalayas"
    # Some himalayas pages embed JSON-LD
    raw, status = get(url, "text/html")
    if not raw:
        return None, status
    # JSON-LD JobPosting
    for m in re.finditer(
        r'<script[^>]+type=\"application/ld\+json\"[^>]*>(.*?)</script>', raw, re.S | re.I
    ):
        try:
            data = json.loads(m.group(1))
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            desc = item.get("description") or ""
            if desc and len(desc) > 200:
                return plain_html(desc)[:MAX_CHARS], "himalayas_jsonld"
    text = plain_html(raw)
    if len(text) >= 800:
        return text[:MAX_CHARS], "himalayas_html"
    return None, "himalayas_thin"


def enrich_one(job: dict) -> tuple[bool, str, int, int]:
    old = len(job.get("description") or "")
    if old >= MIN_OK:
        return False, "already_ok", old, old
    for fn in (from_ashby, from_gh_jid, from_himalayas, from_eures):
        text, status = fn(job)
        if text and len(text) >= MIN_OK:
            job["description"] = text
            job["description_source"] = "remainder_backfill"
            job["description_chars"] = len(text)
            job["description_method"] = status
            return True, status, old, len(text)
        time.sleep(0.1)
    return False, "unresolved", old, old


def main() -> int:
    jobs = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    short = [j for j in jobs if len(j.get("description") or "") < MIN_OK]
    stats = {
        "started": datetime.now(tz=timezone.utc).isoformat(),
        "short_before": len(short),
        "filled": 0,
        "still_short": 0,
        "methods": {},
        "failures": [],
    }
    for i, job in enumerate(short, 1):
        ok, status, before, after = enrich_one(job)
        print(f"[{i}/{len(short)}] {'OK' if ok else 'FAIL'} {before}→{after} {status} · {job.get('job_id','')[:48]}")
        if ok:
            stats["filled"] += 1
            stats["methods"][status] = stats["methods"].get(status, 0) + 1
        else:
            stats["still_short"] += 1
            stats["failures"].append(
                {
                    "job_id": job.get("job_id"),
                    "status": status,
                    "url": job.get("job_url"),
                    "chars": after,
                }
            )

    after_short = sum(1 for j in jobs if len(j.get("description") or "") < MIN_OK)
    lens = [len(j.get("description") or "") for j in jobs]
    stats.update(
        {
            "short_after": after_short,
            "avg_chars": round(sum(lens) / len(lens), 1),
            "max_chars": max(lens),
            "finished": datetime.now(tz=timezone.utc).isoformat(),
        }
    )
    SNAPSHOT.write_text(json.dumps(jobs, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    META.write_text(json.dumps(stats, indent=2), encoding="utf-8")
    print(json.dumps({k: stats[k] for k in ("short_before", "filled", "still_short", "short_after", "avg_chars", "max_chars", "methods")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
