"""
dataforge-company-ingestor

Fetches jobs directly from company career pages via public ATS feeds. The
collector is registry-driven so coverage is not limited to a hand-picked German
company list: add any company to COMPANY_CAREERS_CONFIG or to an S3 JSON config.

Supported public/no-auth feed families:
Greenhouse, Lever, Ashby, Workable, SmartRecruiters, Recruitee, Personio XML,
Workday CXS, Comeet Careers API, and Pinpoint.
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import logging
import re
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import awswrangler as wr
import pandas as pd
import requests


logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = int(os.environ.get("COMPANY_CAREERS_REQUEST_TIMEOUT", "15"))
MAX_WORKERS = int(os.environ.get("COMPANY_CAREERS_MAX_WORKERS", "24"))
MAX_JOBS_PER_COMPANY = int(os.environ.get("COMPANY_CAREERS_MAX_JOBS_PER_COMPANY", "2000"))
FETCH_DETAILS = os.environ.get("COMPANY_CAREERS_FETCH_DETAILS", "false").lower() == "true"

HEADERS = {
    "User-Agent": "DataForge Job Aggregator/1.0 (+public career feed ingestion)",
    "Accept": "application/json, text/plain, */*",
}


# A small global seed keeps local/dev runs useful. Production scale should come
# from COMPANY_CAREERS_CONFIG_S3_URI or COMPANY_CAREERS_CONFIG.
DEFAULT_TARGETS: list[dict[str, Any]] = [
    {"company": "Zalando", "careers_url": "https://boards.greenhouse.io/zalando"},
    {"company": "N26", "careers_url": "https://boards.greenhouse.io/n26group"},
    {"company": "Celonis", "careers_url": "https://boards.greenhouse.io/celonis"},
    {"company": "Personio", "careers_url": "https://boards.greenhouse.io/personio"},
    {"company": "Stripe", "careers_url": "https://boards.greenhouse.io/stripe"},
    {"company": "Databricks", "careers_url": "https://boards.greenhouse.io/databricks"},
    {"company": "Contentful", "careers_url": "https://boards.greenhouse.io/contentful"},
    {"company": "Figma", "careers_url": "https://boards.greenhouse.io/figma"},
    {"company": "Pinterest", "careers_url": "https://boards.greenhouse.io/pinterest"},
    {"company": "Reddit", "careers_url": "https://boards.greenhouse.io/reddit"},
    {"company": "SpaceX", "careers_url": "https://boards.greenhouse.io/spacex"},
    {"company": "Robinhood", "careers_url": "https://boards.greenhouse.io/robinhood"},
    {"company": "Asana", "careers_url": "https://boards.greenhouse.io/asana"},
    {"company": "Twitch", "careers_url": "https://boards.greenhouse.io/twitch"},
    {"company": "Vercel", "careers_url": "https://boards.greenhouse.io/vercel"},
    {"company": "Cloudflare", "careers_url": "https://boards.greenhouse.io/cloudflare"},
    {"company": "Trade Republic", "careers_url": "https://boards.greenhouse.io/traderepublic"},
    {"company": "Airbnb", "careers_url": "https://boards.greenhouse.io/airbnb"},
    {"company": "Palantir", "careers_url": "https://jobs.lever.co/palantir"},
    {"company": "Aircall", "careers_url": "https://jobs.lever.co/aircall"},
    {"company": "Coupa", "careers_url": "https://jobs.lever.co/coupa"},
    {"company": "Anthropic", "careers_url": "https://jobs.ashbyhq.com/Anthropic"},
    {"company": "Ashby", "careers_url": "https://jobs.ashbyhq.com/Ashby"},
    {"company": "Linear", "careers_url": "https://jobs.ashbyhq.com/linear"},
    {"company": "Supabase", "careers_url": "https://jobs.ashbyhq.com/supabase"},
    {"company": "PostHog", "careers_url": "https://jobs.ashbyhq.com/posthog"},
    {"company": "Railway", "careers_url": "https://jobs.ashbyhq.com/railway"},
    {"company": "Modal", "careers_url": "https://jobs.ashbyhq.com/modal"},
    {"company": "Ramp", "careers_url": "https://jobs.ashbyhq.com/ramp"},
    {"company": "Qonto", "ats": "workable", "slug": "qonto"},
    {"company": "Ledger", "ats": "workable", "slug": "ledger"},
    {"company": "WorkMotion", "ats": "workable", "slug": "workmotion"},
    {"company": "Storyteq", "ats": "workable", "slug": "storyteq"},
    {"company": "IT Labs", "ats": "workable", "slug": "it-labs"},
    {"company": "SmartRecruiters", "careers_url": "https://careers.smartrecruiters.com/SmartRecruiters"},
    {"company": "Visa", "careers_url": "https://careers.smartrecruiters.com/Visa"},
    {"company": "Delivery Hero", "careers_url": "https://careers.smartrecruiters.com/DeliveryHero"},
    {"company": "Canva", "careers_url": "https://careers.smartrecruiters.com/Canva"},
    {"company": "Bosch Group", "careers_url": "https://careers.smartrecruiters.com/BoschGroup"},
    {"company": "Bunq", "careers_url": "https://bunq.recruitee.com"},
    {"company": "Workday", "careers_url": "https://workday.wd5.myworkdayjobs.com/Workday"},
    {"company": "Pinpoint", "careers_url": "https://workwithus.pinpointhq.com"},
]


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", html.unescape(str(value))).strip()


def _strip_html(value: Any) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return _clean_text(text)


def _slugify(value: Any) -> str:
    text = _clean_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or "unknown"


def _join_parts(parts: list[Any], sep: str = ", ") -> str:
    return sep.join(_clean_text(p) for p in parts if _clean_text(p))


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "remote", "hybrid", "flex"}


def _looks_remote(*values: Any) -> bool:
    haystack = " ".join(_clean_text(v).lower() for v in values if v is not None)
    return bool(re.search(r"\b(remote|hybrid|flex|anywhere|home[- ]?office|work from home)\b", haystack))


def _job_id(ats: str, slug: str, raw_id: Any, url: str, title: str) -> str:
    token = _clean_text(raw_id) or hashlib.sha1(f"{url}|{title}|{slug}".encode()).hexdigest()[:16]
    return f"direct_{_slugify(ats)}_{_slugify(slug)}_{_slugify(token)}"


def _normalize_job(
    *,
    ats: str,
    slug: str,
    company: str,
    raw_id: Any,
    title: Any,
    location: Any = "",
    url: Any = "",
    description: Any = "",
    job_types: Any = "",
    department: Any = "",
    published_at: Any = "",
    modified_at: Any = "",
    remote: Any = False,
    tags: Any = "",
    salary: Any = "",
) -> dict[str, Any]:
    location_text = _clean_text(location)
    job_type_text = ",".join(_clean_text(item) for item in job_types if _clean_text(item)) if isinstance(job_types, list) else _clean_text(job_types)
    department_text = _clean_text(department)
    tag_text = ",".join(_clean_text(item) for item in tags if _clean_text(item)) if isinstance(tags, list) else _clean_text(tags)
    remote_value = _bool(remote) or _looks_remote(location_text, job_type_text, tag_text)
    title_text = _clean_text(title)
    url_text = _clean_text(url)

    return {
        "job_id": _job_id(ats, slug, raw_id, url_text, title_text),
        "title": title_text,
        "company": _clean_text(company),
        "location": location_text,
        "url": url_text,
        "description": _strip_html(description),
        "source": "direct",
        "ats": _clean_text(ats),
        "job_types": job_type_text,
        "department": department_text,
        "published_at": _clean_text(published_at),
        "modified_at": _clean_text(modified_at or published_at),
        "remote": bool(remote_value),
        "tags": tag_text,
        "salary": _clean_text(salary),
    }


def _request_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    req_headers = {**HEADERS, **(headers or {})}
    response = requests.request(
        method,
        url,
        headers=req_headers,
        params=params,
        json=body,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def _request_text(url: str, *, params: dict[str, Any] | None = None) -> str:
    response = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


def _extract_greenhouse_slug(parsed_url: urlparse) -> dict[str, Any]:
    segments = [s for s in parsed_url.path.split("/") if s]
    return {"slug": segments[-1] if segments else ""}


def _extract_lever_slug(parsed_url: urlparse) -> dict[str, Any]:
    host = parsed_url.netloc.lower()
    segments = [s for s in parsed_url.path.split("/") if s]
    return {
        "slug": segments[0] if segments else "",
        "base_url": "https://api.eu.lever.co" if "jobs.eu.lever.co" in host else "https://api.lever.co",
    }


def _extract_ashby_slug(parsed_url: urlparse) -> dict[str, Any]:
    segments = [s for s in parsed_url.path.split("/") if s]
    return {"slug": segments[0] if segments else ""}


def _extract_workable_slug(parsed_url: urlparse) -> dict[str, Any]:
    segments = [s for s in parsed_url.path.split("/") if s]
    return {"slug": segments[0] if segments else parsed_url.netloc.split(".")[0]}


def _extract_smartrecruiters_slug(parsed_url: urlparse) -> dict[str, Any]:
    segments = [s for s in parsed_url.path.split("/") if s]
    return {"slug": segments[-1] if segments else ""}


def _extract_recruitee_slug(parsed_url: urlparse) -> dict[str, Any]:
    return {"slug": parsed_url.netloc.split(".")[0]}


def _extract_personio_slug(parsed_url: urlparse) -> dict[str, Any]:
    return {"slug": parsed_url.netloc.split(".")[0]}


def _extract_pinpoint_slug(parsed_url: urlparse) -> dict[str, Any]:
    return {"slug": parsed_url.netloc.split(".")[0]}


def _company(entry: dict[str, Any], fallback: str) -> str:
    return _clean_text(entry.get("company") or fallback)


def _detect_ats(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if "greenhouse.io" in host:
        return "greenhouse"
    if "lever.co" in host:
        return "lever"
    if "ashbyhq.com" in host:
        return "ashby"
    if "workable.com" in host:
        return "workable"
    if "smartrecruiters.com" in host:
        return "smartrecruiters"
    if host.endswith(".recruitee.com"):
        return "recruitee"
    if "personio" in host:
        return "personio"
    if "myworkdayjobs.com" in host or "myworkdaysite.com" in host:
        return "workday"
    if host.endswith(".pinpointhq.com"):
        return "pinpoint"
    if "comeet" in host:
        return "comeet"
    raise ValueError(f"Could not detect ATS from careers_url={url!r}; add an explicit ats field.")


def _extract_workday_slug(parsed_url: urlparse) -> dict[str, Any]:
    host = parsed_url.netloc.lower()
    segments = [s for s in parsed_url.path.split("/") if s]
    site_segments = [s for s in segments if not re.fullmatch(r"[a-z]{2}-[A-Z]{2}", s)]
    if "recruiting" in site_segments:
        idx = site_segments.index("recruiting")
        tenant = site_segments[idx + 1] if len(site_segments) > idx + 1 else host.split(".")[0]
        site = site_segments[idx + 2] if len(site_segments) > idx + 2 else ""
    else:
        tenant = host.split(".")[0]
        site = site_segments[-1] if site_segments else ""
    return {"slug": tenant, "tenant": tenant, "site": site, "host": f"{parsed_url.scheme}://{parsed_url.netloc}"}


_SLUG_EXTRACTORS = {
    "greenhouse": _extract_greenhouse_slug,
    "lever": _extract_lever_slug,
    "ashby": _extract_ashby_slug,
    "workable": _extract_workable_slug,
    "smartrecruiters": _extract_smartrecruiters_slug,
    "recruitee": _extract_recruitee_slug,
    "personio": _extract_personio_slug,
    "pinpoint": _extract_pinpoint_slug,
    "workday": _extract_workday_slug,
}

def _normalize_target(entry: dict[str, Any]) -> dict[str, Any]:
    target = dict(entry)
    if "ats" not in target and target.get("careers_url"):
        target["ats"] = _detect_ats(target["careers_url"])
    if target.get("careers_url") and target["ats"] in _SLUG_EXTRACTORS:
        inferred = _SLUG_EXTRACTORS[target["ats"]](urlparse(target["careers_url"]))
        target = {**inferred, **target}
    target["ats"] = _clean_text(target.get("ats")).lower()
    target["slug"] = _clean_text(target.get("slug"))
    if not target["ats"]:
        raise ValueError(f"Company target is missing ats: {entry}")
    if target["ats"] != "workday" and not target["slug"]:
        raise ValueError(f"Company target is missing slug: {entry}")
    return target


def fetch_greenhouse(entry: dict[str, Any]) -> list[dict[str, Any]]:
    slug = entry["slug"]
    company = _company(entry, slug)
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    data = _request_json(url, params={"content": "true"})
    jobs = data.get("jobs", [])

    results = []
    for job in jobs:
        departments = job.get("departments") or []
        department = _join_parts([d.get("name") for d in departments if isinstance(d, dict)])
        results.append(_normalize_job(
            ats="greenhouse",
            slug=slug,
            company=company,
            raw_id=job.get("id"),
            title=job.get("title"),
            location=(job.get("location") or {}).get("name", ""),
            url=job.get("absolute_url", ""),
            description=job.get("content", ""),
            department=department,
            modified_at=job.get("updated_at", ""),
        ))
    return results


def fetch_lever(entry: dict[str, Any]) -> list[dict[str, Any]]:
    slug = entry["slug"]
    company = _company(entry, slug)
    base_url = entry.get("base_url") or "https://api.lever.co"
    limit = 250
    skip = 0
    results: list[dict[str, Any]] = []

    while len(results) < MAX_JOBS_PER_COMPANY:
        url = f"{base_url}/v0/postings/{slug}"
        jobs = _request_json(url, params={"mode": "json", "limit": limit, "skip": skip})
        if not isinstance(jobs, list) or not jobs:
            break

        for job in jobs:
            categories = job.get("categories") or {}
            location = categories.get("location") or _join_parts(categories.get("allLocations") or [])
            salary = job.get("salaryDescriptionPlain") or job.get("salaryDescription") or ""
            results.append(_normalize_job(
                ats="lever",
                slug=slug,
                company=company,
                raw_id=job.get("id"),
                title=job.get("text"),
                location=location,
                url=job.get("hostedUrl") or job.get("applyUrl", ""),
                description=job.get("descriptionPlain") or job.get("description") or "",
                job_types=categories.get("commitment", ""),
                department=categories.get("department") or categories.get("team", ""),
                published_at=job.get("createdAt", ""),
                remote=job.get("workplaceType", ""),
                tags=[categories.get("team", ""), categories.get("level", "")],
                salary=salary,
            ))

        if len(jobs) < limit:
            break
        skip += len(jobs)
    return results


def fetch_ashby(entry: dict[str, Any]) -> list[dict[str, Any]]:
    slug = entry["slug"]
    company = _company(entry, slug)
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    data = _request_json(url, params={"includeCompensation": "true"})
    results = []

    for job in data.get("jobs", []):
        if job.get("isListed") is False and not entry.get("include_unlisted", False):
            continue
        compensation = job.get("compensation") or {}
        results.append(_normalize_job(
            ats="ashby",
            slug=slug,
            company=company,
            raw_id=job.get("id") or job.get("jobUrl"),
            title=job.get("title"),
            location=job.get("location", ""),
            url=job.get("jobUrl") or job.get("applyUrl", ""),
            description=job.get("descriptionPlain") or job.get("descriptionHtml", ""),
            job_types=job.get("employmentType", ""),
            department=job.get("department") or job.get("team", ""),
            published_at=job.get("publishedAt", ""),
            remote=job.get("isRemote") or job.get("workplaceType", ""),
            salary=compensation.get("scrapeableCompensationSalarySummary")
            or compensation.get("compensationTierSummary", ""),
        ))
    return results


def fetch_workable(entry: dict[str, Any]) -> list[dict[str, Any]]:
    slug = entry["slug"]
    company = _company(entry, slug)
    url = f"https://www.workable.com/api/accounts/{slug}"
    data = _request_json(url, params={"details": "true"})
    jobs = data.get("jobs", data if isinstance(data, list) else [])
    account_name = data.get("name", "") if isinstance(data, dict) else ""
    results = []

    for job in jobs:
        location = job.get("location")
        if isinstance(location, dict):
            location_text = location.get("location_str") or _join_parts([
                location.get("city"),
                location.get("region"),
                location.get("country"),
            ])
            remote = location.get("telecommuting") or location.get("workplace_type", "")
        else:
            location_text = location or _join_parts([job.get("city"), job.get("country")])
            remote = job.get("telecommuting", False)
        salary = job.get("salary")
        if isinstance(salary, dict):
            salary = _join_parts([salary.get("salary_from"), salary.get("salary_to"), salary.get("salary_currency")], " ")

        results.append(_normalize_job(
            ats="workable",
            slug=slug,
            company=company or account_name or slug,
            raw_id=job.get("shortcode") or job.get("id"),
            title=job.get("title") or job.get("full_title"),
            location=location_text,
            url=job.get("url") or job.get("shortlink") or job.get("application_url", ""),
            description=job.get("description") or job.get("full_description", ""),
            job_types=job.get("employment_type", ""),
            department=job.get("department", ""),
            published_at=job.get("published_on") or job.get("created_at", ""),
            remote=remote,
            salary=salary or "",
        ))
    return results


def _smartrecruiters_detail(slug: str, posting_id: Any) -> dict[str, Any]:
    detail_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings/{posting_id}"
    try:
        return _request_json(detail_url)
    except Exception:
        return {}


def fetch_smartrecruiters(entry: dict[str, Any]) -> list[dict[str, Any]]:
    slug = entry["slug"]
    company = _company(entry, slug)
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    offset = 0
    limit = 100
    results = []

    while len(results) < MAX_JOBS_PER_COMPANY:
        data = _request_json(url, params={"limit": limit, "offset": offset})
        page = data.get("content", [])
        if not page:
            break

        for job in page:
            detail = _smartrecruiters_detail(slug, job.get("id")) if (FETCH_DETAILS or entry.get("fetch_details")) else {}
            source = detail or job
            loc = source.get("location") or {}
            description = ""
            sections = (detail.get("jobAd") or {}).get("sections") or {}
            if sections:
                description = " ".join(
                    _clean_text(section.get("text", ""))
                    for section in sections.values()
                    if isinstance(section, dict)
                )
            results.append(_normalize_job(
                ats="smartrecruiters",
                slug=slug,
                company=company or (source.get("company") or {}).get("name", ""),
                raw_id=source.get("id") or source.get("uuid"),
                title=source.get("name"),
                location=_join_parts([loc.get("city"), loc.get("region"), loc.get("country")]),
                url=detail.get("applyUrl") or f"https://jobs.smartrecruiters.com/{slug}/{job.get('id')}",
                description=description,
                job_types=(source.get("typeOfEmployment") or {}).get("label", ""),
                department=(source.get("department") or {}).get("label", ""),
                published_at=source.get("releasedDate", ""),
                remote=loc.get("remote", False),
            ))

        if len(page) < limit:
            break
        offset += limit
    return results


def fetch_recruitee(entry: dict[str, Any]) -> list[dict[str, Any]]:
    slug = entry["slug"]
    company = _company(entry, slug)
    data = _request_json(f"https://{slug}.recruitee.com/api/offers/")
    results = []

    for job in data.get("offers", []):
        results.append(_normalize_job(
            ats="recruitee",
            slug=slug,
            company=company or job.get("company_name", ""),
            raw_id=job.get("id") or job.get("guid") or job.get("slug"),
            title=job.get("title"),
            location=job.get("location") or _join_parts([job.get("city"), job.get("state_name"), job.get("country")]),
            url=job.get("careers_url") or job.get("careers_apply_url", ""),
            description=_join_parts([job.get("description"), job.get("requirements")], "\n"),
            job_types=job.get("employment_type") or job.get("employment_type_code", ""),
            department=job.get("department", ""),
            published_at=job.get("published_at") or job.get("created_at", ""),
            modified_at=job.get("updated_at", ""),
            remote=job.get("remote", False),
            tags=job.get("tags", []),
            salary=job.get("salary", ""),
        ))
    return results


def fetch_personio(entry: dict[str, Any]) -> list[dict[str, Any]]:
    slug = entry["slug"]
    company = _company(entry, slug)
    params = {"language": entry.get("language", "en")} if entry.get("language", "en") else None
    text = _request_text(f"https://{slug}.jobs.personio.com/xml", params=params)
    root = ET.fromstring(text)
    results = []

    for job in list(root):
        fields = {
            child.tag.split("}", 1)[-1].lower(): _clean_text("".join(child.itertext()))
            for child in list(job)
        }
        raw_id = fields.get("id") or fields.get("jobid") or fields.get("requisitionid") or fields.get("name")
        title = fields.get("name") or fields.get("title") or fields.get("jobtitle")
        location = fields.get("office") or fields.get("location") or _join_parts([fields.get("city"), fields.get("country")])
        description = _join_parts([
            fields.get("jobdescription"),
            fields.get("description"),
            fields.get("profile"),
            fields.get("recruitingcategory"),
        ], "\n")
        results.append(_normalize_job(
            ats="personio",
            slug=slug,
            company=company or fields.get("company", ""),
            raw_id=raw_id,
            title=title,
            location=location,
            url=fields.get("url") or f"https://{slug}.jobs.personio.com/job/{raw_id}",
            description=description,
            job_types=fields.get("employmenttype") or fields.get("schedule"),
            department=fields.get("department", ""),
            published_at=fields.get("createdat") or fields.get("publishedat", ""),
            modified_at=fields.get("updatedat", ""),
            remote=fields.get("workplace") == "remote" or _looks_remote(location, description),
        ))
    return [job for job in results if job["title"]]


def fetch_workday(entry: dict[str, Any]) -> list[dict[str, Any]]:
    host = entry.get("host")
    tenant = entry.get("tenant") or entry.get("slug")
    site = entry.get("site")
    if not (host and tenant and site):
        raise ValueError("Workday targets require host, tenant, and site or a parseable careers_url.")

    company = _company(entry, tenant)
    api_base = f"{host}/wday/cxs/{tenant}/{site}"
    public_base = entry.get("careers_url") or f"{host}/{site}"
    offset = 0
    limit = int(entry.get("limit", 100))
    results = []

    while len(results) < MAX_JOBS_PER_COMPANY:
        data = _request_json(
            f"{api_base}/jobs",
            method="POST",
            headers={"Content-Type": "application/json"},
            body={"appliedFacets": entry.get("applied_facets", {}), "limit": limit, "offset": offset, "searchText": entry.get("search_text", "")},
        )
        page = data.get("jobPostings", [])
        if not page:
            break

        for job in page:
            detail = {}
            if FETCH_DETAILS or entry.get("fetch_details", True):
                try:
                    detail = _request_json(f"{api_base}{job.get('externalPath', '')}")
                except Exception:
                    detail = {}
            info = detail.get("jobPostingInfo") or {}
            external_path = job.get("externalPath") or info.get("externalUrl", "")
            locations = [info.get("location") or job.get("locationsText")]
            locations.extend(info.get("additionalLocations") or [])
            results.append(_normalize_job(
                ats="workday",
                slug=tenant,
                company=company,
                raw_id=info.get("jobReqId") or info.get("id") or (job.get("bulletFields") or [""])[0] or external_path,
                title=info.get("title") or job.get("title"),
                location=_join_parts(locations),
                url=info.get("externalUrl") or f"{public_base}{external_path}",
                description=info.get("jobDescription", ""),
                job_types=info.get("timeType", ""),
                department=info.get("jobRequisitionLocation", {}).get("descriptor", "") if isinstance(info.get("jobRequisitionLocation"), dict) else "",
                published_at=info.get("startDate") or info.get("postedOn") or job.get("postedOn", ""),
                remote=info.get("remoteType") or job.get("remoteType", ""),
            ))

        total = data.get("total", 0)
        offset += len(page)
        if len(page) < limit or (total and offset >= total):
            break
    return results


def fetch_comeet(entry: dict[str, Any]) -> list[dict[str, Any]]:
    slug = entry.get("company_uid") or entry["slug"]
    company = _company(entry, slug)
    url = f"https://www.comeet.co/careers-api/2.0/company/{slug}/positions"
    data = _request_json(url, params={"details": "true"})
    jobs = data.get("positions", data if isinstance(data, list) else [])
    results = []

    for job in jobs:
        loc = job.get("location") or {}
        location = loc.get("name") if isinstance(loc, dict) else loc
        results.append(_normalize_job(
            ats="comeet",
            slug=slug,
            company=company,
            raw_id=job.get("uid") or job.get("id"),
            title=job.get("name") or job.get("title"),
            location=location,
            url=job.get("url") or job.get("absolute_url") or job.get("position_url", ""),
            description=job.get("description") or job.get("details", ""),
            job_types=job.get("employment_type") or job.get("type", ""),
            department=job.get("department", ""),
            published_at=job.get("time_created") or job.get("published_at", ""),
            modified_at=job.get("time_updated", ""),
        ))
    return results


def fetch_pinpoint(entry: dict[str, Any]) -> list[dict[str, Any]]:
    slug = entry["slug"]
    company = _company(entry, slug)
    data = _request_json(f"https://{slug}.pinpointhq.com/postings.json")
    jobs = data.get("data", data if isinstance(data, list) else [])
    results = []

    for job in jobs:
        location = job.get("location")
        if isinstance(location, dict):
            location = location.get("name") or _join_parts([location.get("city"), location.get("country")])
        description = _join_parts([
            job.get("description"),
            job.get("key_responsibilities"),
            job.get("skills_knowledge_expertise"),
            job.get("benefits"),
        ], "\n")
        salary = job.get("compensation")
        if not salary and job.get("compensation_visible"):
            salary = _join_parts([
                job.get("compensation_minimum"),
                job.get("compensation_maximum"),
                job.get("compensation_currency"),
                job.get("compensation_frequency"),
            ], " ")
        results.append(_normalize_job(
            ats="pinpoint",
            slug=slug,
            company=company,
            raw_id=job.get("id") or job.get("path"),
            title=job.get("title"),
            location=location,
            url=job.get("url") or f"https://{slug}.pinpointhq.com{job.get('path', '')}",
            description=description,
            job_types=job.get("employment_type_text") or job.get("employment_type", ""),
            department=job.get("department") or (job.get("job") or {}).get("department", ""),
            published_at=job.get("published_at") or job.get("created_at", ""),
            remote=job.get("workplace_type_text") or job.get("workplace_type", ""),
            salary=salary or "",
        ))
    return results


FETCHER_MAP = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "workable": fetch_workable,
    "smartrecruiters": fetch_smartrecruiters,
    "recruitee": fetch_recruitee,
    "personio": fetch_personio,
    "workday": fetch_workday,
    "comeet": fetch_comeet,
    "pinpoint": fetch_pinpoint,
}


def _parse_targets_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        payload = payload.get("companies") or payload.get("targets") or []
    if not isinstance(payload, list):
        raise ValueError("Company careers config must be a list or an object with companies/targets.")
    return [_normalize_target(item) for item in payload if isinstance(item, dict)]


def _load_s3_json(uri: str) -> Any:
    import boto3

    match = re.fullmatch(r"s3://([^/]+)/(.+)", uri)
    if not match:
        raise ValueError("COMPANY_CAREERS_CONFIG_S3_URI must look like s3://bucket/key.json")
    bucket, key = match.groups()
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def load_company_targets() -> list[dict[str, Any]]:
    use_defaults = os.environ.get("COMPANY_CAREERS_USE_DEFAULTS", "true").lower() != "false"
    targets = [_normalize_target(t) for t in DEFAULT_TARGETS] if use_defaults else []

    s3_uri = os.environ.get("COMPANY_CAREERS_CONFIG_S3_URI", "").strip()
    config_url = os.environ.get("COMPANY_CAREERS_CONFIG_URL", "").strip()
    inline = os.environ.get("COMPANY_CAREERS_CONFIG", "").strip()
    mode = os.environ.get("COMPANY_CAREERS_CONFIG_MODE", "append").lower()

    loaded: list[dict[str, Any]] = []
    if s3_uri:
        loaded.extend(_parse_targets_payload(_load_s3_json(s3_uri)))
    if config_url:
        try:
            res = requests.get(config_url, timeout=10, headers=HEADERS)
            res.raise_for_status()
            loaded.extend(_parse_targets_payload(res.json()))
            logger.info(f"Loaded {len(loaded)} targets from remote URL: {config_url}")
        except Exception as e:
            logger.error(f"Failed to load targets from remote URL {config_url}: {e}", exc_info=True)
    if inline:
        loaded.extend(_parse_targets_payload(json.loads(inline)))

    if loaded and mode == "replace":
        targets = loaded
    else:
        targets.extend(loaded)

    deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for target in targets:
        key = (target.get("ats", ""), target.get("slug", ""), target.get("site", ""))
        deduped[key] = target
    return list(deduped.values())


def _fetch_one(entry: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    label = f"{entry.get('ats')}/{entry.get('slug') or entry.get('tenant')}"
    try:
        fetcher = FETCHER_MAP[entry["ats"]]
        jobs: list[dict[str, Any]] = fetcher(entry)
        logger.info(f"Fetched {len(jobs)} jobs from {label}")
        return label, jobs
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        if status == 404:
            logger.warning(f"Skipping {label}: 404 Not Found")
        else:
            logger.error(f"HTTP Error for {label}: Status {status}", exc_info=True)
        return label, []
    except Exception as exc:
        logger.error(f"Unexpected error for {label}: {type(exc).__name__}: {exc}", exc_info=True)
        return label, []


def collect_company_jobs(targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    all_jobs: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, max(len(targets), 1))) as executor:
        futures = {executor.submit(_fetch_one, target): target for target in targets}
        for future in as_completed(futures):
            _, jobs = future.result()
            for job in jobs:  # type: ignore
                job_id = job.get("job_id", "")
                if job_id and job_id not in seen_ids and job.get("title"):
                    seen_ids.add(job_id)
                    all_jobs.append(job)
    return all_jobs


def lambda_handler(event, context):
    bucket = os.environ.get("BRONZE_BUCKET")
    if not bucket:
        raise ValueError("BRONZE_BUCKET environment variable is not set.")

    targets = load_company_targets()
    if not targets: # type: ignore
        return {"statusCode": 204, "body": "No company career targets configured."}

    logger.info(
        f"Starting company careers ingestor: {len(targets)} companies across "
        f"{len({t['ats'] for t in targets})} ATS platforms" # type: ignore
    )
    started = time.time()
    all_jobs = collect_company_jobs(targets)
    print(f"Fetched {len(all_jobs)} unique direct jobs in {time.time() - started:.1f}s")

    if not all_jobs:
        return {"statusCode": 204, "body": "No jobs found from company career pages."}

    df = pd.DataFrame(all_jobs)
    required_cols = [
        "job_id", "title", "company", "location", "url", "description", "source",
        "ats", "job_types", "department", "published_at", "modified_at", "remote",
        "tags", "salary",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = False if col == "remote" else ""

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("").astype(str)
    df["remote"] = df["remote"].fillna(False).astype(bool)
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"s3://{bucket}/direct_careers/ingested_at={date_str}/jobs.parquet"
    wr.s3.to_parquet(df=df, path=path, index=False)

    by_ats = df.groupby("ats").size().sort_values(ascending=False).to_dict()
    body = {
        "message": f"Company careers ingestor complete; wrote {len(df)} jobs to {path}",
        "total_jobs": len(df),
        "targets": len(targets),
        "by_ats": by_ats,
    } # type: ignore
    logger.info(json.dumps(body))
    return {"statusCode": 200, "body": json.dumps(body)}
