import hashlib
import re
import requests
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from .typing_inspection.arbeitnow import validate_api_response as validate_arbeitnow
from .company_normalize import normalize_company


# Title-first tech role patterns (primary gate)
_TITLE_TECH_RE = re.compile(
    r"\b("
    r"engineer|developer|analyst|scientist|architect|"
    r"ml|ai|data|backend|frontend|fullstack|full[\s-]?stack|"
    r"sre|devops|platform|software|cloud"
    r")\b",
    re.IGNORECASE,
)

# Secondary: description-only when title is weak
_DESC_TECH_RE = re.compile(
    r"\b(analytics|business\s+intelligence|\bbi\b|machine\s+learning|data\s+engineer)\b",
    re.IGNORECASE,
)


def _is_tech_job(title: str, description: str) -> bool:
    title_lower = (title or "").lower()
    if _TITLE_TECH_RE.search(title_lower):
        return True
    desc_lower = (description or "").lower()
    return bool(_DESC_TECH_RE.search(desc_lower))


def _company_from_berlin_slug(link: str) -> Optional[str]:
    """Extract employer name from Berlin Startup Jobs URL slug (company suffix)."""
    try:
        slug = urlparse(link).path.strip("/").split("/")[-1]
        if not slug:
            return None
        parts = slug.split("-")
        best: Optional[str] = None
        for n in range(1, min(6, len(parts) + 1)):
            chunk = parts[-n:]
            name = " ".join(w.capitalize() for w in chunk)
            company = normalize_company(name)
            if company:
                best = company
        return best
    except Exception:
        return None


def _parse_title_company(title_text: str, link: str) -> Tuple[str, Optional[str]]:
    """Extract job title and company from RSS title patterns."""
    title = title_text.strip()
    company: Optional[str] = None

    # 1. Explicit separators in RSS title (highest priority)
    for sep in (" // ", " — ", " – ", " - ", " at ", " @ ", " | "):
        if sep in title_text:
            parts = title_text.rsplit(sep, 1)
            title = parts[0].strip()
            company = parts[1].strip()
            break

    company = normalize_company(company)
    if company:
        return title, company

    # 2. URL slug suffix (Berlin Startup Jobs puts company at end of slug)
    company = _company_from_berlin_slug(link)
    if company:
        return title, company

    return title, None


class ArbeitnowFetcher:
    """Fetcher for the Arbeitnow public job board API."""

    API_URL = "https://www.arbeitnow.com/api/job-board-api"
    MAX_PAGES = 2

    def fetch_jobs(self) -> Dict[str, Any]:
        print("Fetching data from Arbeitnow API...")
        all_jobs = []
        page = 1

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        while page <= self.MAX_PAGES:
            response = requests.get(self.API_URL, params={"page": page}, headers=headers, timeout=10)
            response.raise_for_status()
            raw_data = response.json()
            validated = validate_arbeitnow(raw_data)
            jobs = validated.get("data", [])
            if not jobs:
                break
            all_jobs.extend(jobs)
            next_page = validated.get("links", {}).get("next")
            if not next_page:
                break
            page += 1

        print(f"Fetched {len(all_jobs)} total jobs from Arbeitnow across {page} page(s).")
        return {"data": all_jobs}


class BAFetcher:
    """Fetcher for the Bundesagentur fur Arbeit public Jobsuche API (no auth required)."""

    JOBS_URL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobs"

    def fetch_jobs(self, query: str = "Data Engineer") -> Dict[str, Any]:
        """Fetches all pages of jobs from BA public API."""
        headers = {"X-API-Key": "jobboerse-jobsuche", "Accept": "application/json"}

        all_jobs = []
        page = 1
        page_size = 100

        while True:
            params = {"was": query, "size": page_size, "page": page}
            print(f"Fetching BA API page {page} for query: {query}")
            response = requests.get(self.JOBS_URL, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            raw_data = response.json()
            jobs = raw_data.get("stellenangebote", [])
            if not jobs:
                break
            all_jobs.extend(jobs)
            max_results = raw_data.get("maxErgebnisse", 0)
            if len(all_jobs) >= max_results:
                break
            page += 1

        print(f"Fetched {len(all_jobs)} total jobs from BA API for '{query}'.")
        return {"stellenangebote": all_jobs}


def get_fetcher(source: str) -> Any:
    """Factory function to get the appropriate fetcher."""
    if source == "arbeitnow":
        return ArbeitnowFetcher()
    if source == "ba":
        return BAFetcher()
    if source == "berlin_startups":
        return BerlinStartupJobsFetcher()
    raise ValueError(f"Unknown source: {source}")


class BerlinStartupJobsFetcher:
    """Fetcher for Berlin Startup Jobs RSS feeds (tech-focused categories only)."""

    FEED_URLS: List[str] = [
        "https://berlinstartupjobs.com/feed/",
        "https://berlinstartupjobs.com/engineering/feed/",
        "https://berlinstartupjobs.com/product-management/feed/",
        "https://berlinstartupjobs.com/internships/feed/",
    ]

    def fetch_jobs(self) -> Dict[str, Any]:
        """Fetches and parses tech jobs from Berlin Startup Jobs RSS feeds."""
        print("Fetching Berlin Startup Jobs RSS feeds...")
        jobs = []
        seen_links = set()

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/xml, text/xml, */*",
        }

        for url in self.FEED_URLS:
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                root = ET.fromstring(response.content)
                items = root.findall(".//item")
                print(f"Loaded {len(items)} items from feed: {url}")

                for item in items:
                    title_el = item.find("title")
                    link_el = item.find("link")
                    desc_el = item.find("description")

                    if title_el is None or link_el is None:
                        continue

                    title_text = title_el.text or ""
                    link = link_el.text or ""
                    description = desc_el.text or "" if desc_el is not None else ""

                    if link in seen_links:
                        continue
                    seen_links.add(link)

                    title, company = _parse_title_company(title_text, link)
                    if not company:
                        continue

                    if not _is_tech_job(title, description):
                        continue

                    desc_lower = description.lower()
                    title_lower = title.lower()

                    remote = False
                    if any(k in title_lower for k in ("remote", "home office", "home-office")):
                        remote = True
                    elif any(
                        k in desc_lower
                        for k in ("remote", "home office", "work from home", "mobiles arbeiten")
                    ):
                        remote = True

                    location = "Berlin"
                    if remote:
                        if "hybrid" in desc_lower or "hybrid" in title_lower:
                            location = "Berlin (Hybrid)"
                        else:
                            location = "Remote / Berlin"

                    job_hash = hashlib.sha256(link.encode()).hexdigest()[:12]

                    jobs.append(
                        {
                            "job_id": f"bsj_{job_hash}",
                            "title": title,
                            "company": company,
                            "location": location,
                            "url": link,
                            "description": description,
                            "remote": remote,
                            "tags": "Berlin,tech",
                            "job_types": "full_time",
                            "source": "berlin_startups",
                        }
                    )
            except Exception as e:
                print(f"Warning: Failed to fetch/parse feed {url}: {str(e)}")

        print(f"Fetched {len(jobs)} total tech/data jobs from Berlin Startup Jobs.")
        return {"data": jobs}
