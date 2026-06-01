import requests
import re
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict
from .typing_inspection.arbeitnow import validate_api_response as validate_arbeitnow

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
            "Accept": "application/json"
        }
        while page <= self.MAX_PAGES:
            response = requests.get(self.API_URL, params={"page": page}, headers=headers, timeout=10)
            response.raise_for_status()
            raw_data = response.json()
            validated = validate_arbeitnow(raw_data)
            jobs = validated.get('data', [])
            if not jobs:
                break
            all_jobs.extend(jobs)
            # Stop if there are no more pages
            next_page = validated.get('links', {}).get('next')
            if not next_page:
                break
            page += 1

        print(f"Fetched {len(all_jobs)} total jobs from Arbeitnow across {page} page(s).")
        return {'data': all_jobs}

class BAFetcher:
    """Fetcher for the Bundesagentur fur Arbeit public Jobsuche API (no auth required)."""

    JOBS_URL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobs"

    def __init__(self, ssm_parameter_name: str = None):
        pass  # No credentials needed for public API

    def fetch_jobs(self, query: str = "Data Engineer") -> Dict[str, Any]:
        """Fetches all pages of jobs from BA public API."""
        headers = {
            "X-API-Key": "jobboerse-jobsuche",
            "Accept": "application/json"
        }

        all_jobs = []
        page = 1
        page_size = 100

        while True:
            params = {"was": query, "size": page_size, "page": page}
            print(f"Fetching BA API page {page} for query: {query}")
            response = requests.get(self.JOBS_URL, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            raw_data = response.json()
            jobs = raw_data.get('stellenangebote', [])
            if not jobs:
                break
            all_jobs.extend(jobs)
            max_results = raw_data.get('maxErgebnisse', 0)
            if len(all_jobs) >= max_results:
                break
            page += 1

        print(f"Fetched {len(all_jobs)} total jobs from BA API for '{query}'.")
        return {'stellenangebote': all_jobs}

def get_fetcher(source: str) -> Any:
    """Factory function to get the appropriate fetcher."""
    if source == "arbeitnow":
        return ArbeitnowFetcher()
    elif source == "ba":
        return BAFetcher()
    elif source == "hacker_news":
        return HackerNewsFetcher()
    elif source == "berlin_startups":
        return BerlinStartupJobsFetcher()
    else:
        raise ValueError(f"Unknown source: {source}")


class HackerNewsFetcher:
    """Fetcher for the Hacker News API (jobstories and monthly whoishiring)."""
    
    BASE_URL = "https://hacker-news.firebaseio.com/v0"
    
    def fetch_jobs(self) -> Dict[str, Any]:
        """Fetches latest job postings from Hacker News."""
        print("Fetching Hacker News jobstories...")
        jobs = []
        
        # 1. Fetch direct jobstories (funded YC startup posts)
        try:
            response = requests.get(f"{self.BASE_URL}/jobstories.json", timeout=10)
            response.raise_for_status()
            story_ids = response.json()[:35]  # limit to latest 35 to avoid high API latency
            for story_id in story_ids:
                try:
                    res = requests.get(f"{self.BASE_URL}/item/{story_id}.json", timeout=5)
                    res.raise_for_status()
                    item = res.json()
                    if item and not item.get('deleted') and not item.get('dead'):
                        title_text = item.get('title', '')
                        
                        # Extract YC batch tags from title (e.g. "(YC F24)")
                        yc_batch = ""
                        batch_match = re.search(r'\b(YC [WSF]?\d{2})\b', title_text)
                        if batch_match:
                            yc_batch = batch_match.group(1)
                        
                        clean_title_text = re.sub(r'\(\s*YC [WSF]?\d{2}\s*\)', '', title_text).strip()
                        
                        company = "Hacker News Startup"
                        title = clean_title_text
                        
                        match = re.search(r'^(.*?)\b(is hiring|Is Hiring|hiring|Hiring)\b\s*(a|an)?\s*(.*)$', clean_title_text, re.IGNORECASE)
                        if match:
                            company = match.group(1).strip()
                            title = match.group(4).strip()
                        
                        if not title:
                            title = "Startup Opportunities"
                        
                        if title.lower().startswith("in "):
                            title = title[3:].strip()
                        elif title.lower().startswith("for "):
                            title = title[4:].strip()
                            
                        if title:
                            title = title[0].upper() + title[1:]
                        
                        location = "Remote"
                        remote = True
                        
                        loc_match = re.search(r'\[(.*?)\]', clean_title_text)
                        if not loc_match:
                            loc_match = re.search(r'\((.*?)\)', clean_title_text)
                            
                        if loc_match:
                            location_str = loc_match.group(1).strip()
                            location = location_str
                            remote = 'remote' in location_str.lower() or ('onsite' not in location_str.lower() and 'on-site' not in location_str.lower())
                        else:
                            if 'remote' in clean_title_text.lower():
                                remote = True
                                location = "Remote"
                            elif 'on-site' in clean_title_text.lower() or 'onsite' in clean_title_text.lower():
                                remote = False
                                location = "On-site"
                                
                        tags_list = ['Startup', 'YC']
                        if yc_batch:
                            tags_list.append(yc_batch)
                        
                        jobs.append({
                            'job_id': f"hn_job_{story_id}",
                            'title': title,
                            'company': company,
                            'location': location,
                            'url': item.get('url', f"https://news.ycombinator.com/item?id={story_id}"),
                            'description': title_text,
                            'remote': remote,
                            'tags': ','.join(tags_list),
                            'job_types': 'full_time',
                            'source': 'hacker_news'
                        })
                except Exception as e:
                    print(f"Warning: Failed to fetch HN item {story_id}: {str(e)}")
        except Exception as e:
            print(f"Warning: Failed to fetch HN jobstories list: {str(e)}")
            
        # 2. Fetch comments from latest "Who is hiring" thread
        try:
            user_res = requests.get(f"{self.BASE_URL}/user/whoishiring.json", timeout=10)
            user_res.raise_for_status()
            submitted = user_res.json().get('submitted', [])[:8]
            
            hiring_thread_id = None
            for item_id in submitted:
                try:
                    res = requests.get(f"{self.BASE_URL}/item/{item_id}.json", timeout=5)
                    res.raise_for_status()
                    item = res.json()
                    if item and 'Who is hiring?' in item.get('title', ''):
                        hiring_thread_id = item_id
                        print(f"Found latest HN 'Who is hiring' thread: {item.get('title')} (ID: {hiring_thread_id})")
                        break
                except Exception:
                    continue
            
            if hiring_thread_id:
                thread_res = requests.get(f"{self.BASE_URL}/item/{hiring_thread_id}.json", timeout=10)
                thread_res.raise_for_status()
                comment_ids = thread_res.json().get('kids', [])[:60]  # fetch top 60 comments to stay performant
                
                for comment_id in comment_ids:
                    try:
                        res = requests.get(f"{self.BASE_URL}/item/{comment_id}.json", timeout=5)
                        res.raise_for_status()
                        comment = res.json()
                        if comment and not comment.get('deleted') and not comment.get('dead'):
                            text = comment.get('text', '')
                            # Clean HTML tags and decode basic entities
                            plain_text = re.sub(r'<[^>]+>', ' ', text).replace('&quot;', '"').replace('&#x2F;', '/').replace('&amp;', '&').strip()
                            
                            lines = [l.strip() for l in plain_text.split('\n') if l.strip()]
                            if not lines:
                                continue
                            first_line = lines[0]
                            
                            text_lower = plain_text.lower()
                            # Filter for jobs relevant to Germany, Europe, or Remote
                            is_relevant = any(w in text_lower for w in ['germany', 'berlin', 'munich', 'hamburg', 'frankfurt', 'europe', 'remote', 'worldwide', 'eu'])
                            if not is_relevant:
                                continue
                            
                            parts = [p.strip() for p in re.split(r'[|;\-]', first_line)]
                            if len(parts) >= 2:
                                company = parts[0]
                                title = parts[1]
                                location = parts[2] if len(parts) > 2 else "Remote"
                                
                                links = re.findall(r'href="([^"]+)"', text)
                                apply_url = links[0] if links else f"https://news.ycombinator.com/item?id={comment_id}"
                                
                                remote = 'remote' in location.lower() or 'remote' in first_line.lower()
                                
                                jobs.append({
                                    'job_id': f"hn_comment_{comment_id}",
                                    'title': title,
                                    'company': company,
                                    'location': location,
                                    'url': apply_url,
                                    'description': plain_text,
                                    'remote': remote,
                                    'tags': 'Startup,HN Hiring',
                                    'job_types': 'full_time',
                                    'source': 'hacker_news'
                                })
                    except Exception as e:
                        print(f"Warning: Failed to fetch comment {comment_id}: {str(e)}")
        except Exception as e:
            print(f"Warning: Failed to fetch HN hiring comments: {str(e)}")
            
        print(f"Fetched {len(jobs)} total jobs from Hacker News.")
        return {'data': jobs}


class BerlinStartupJobsFetcher:
    """Fetcher for the Berlin Startup Jobs RSS feed."""
    
    FEED_URLS = [
        "https://berlinstartupjobs.com/feed/",
        "https://berlinstartupjobs.com/engineering/feed/"
    ]
    
    def fetch_jobs(self) -> Dict[str, Any]:
        """Fetches and parses the latest jobs from Berlin Startup Jobs RSS feed."""
        print("Fetching Berlin Startup Jobs RSS feeds...")
        import xml.etree.ElementTree as ET
        
        jobs = []
        seen_links = set()
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/xml, text/xml, */*"
        }
        
        for url in self.FEED_URLS:
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                
                # Parse XML
                root = ET.fromstring(response.content)
                items = root.findall('.//item')
                print(f"Loaded {len(items)} items from feed: {url}")
                
                for item in items:
                    title_el = item.find('title')
                    link_el = item.find('link')
                    desc_el = item.find('description')
                    
                    if title_el is None or link_el is None:
                        continue
                        
                    title_text = title_el.text or ""
                    link = link_el.text or ""
                    description = desc_el.text or ""
                    
                    if link in seen_links:
                        continue
                    seen_links.add(link)
                    
                    # Parse company and title
                    company = "Startup"
                    title = title_text
                    
                    if " at " in title_text:
                        parts = title_text.rsplit(" at ", 1)
                        title = parts[0].strip()
                        company = parts[1].strip()
                    elif " – " in title_text:
                        parts = title_text.rsplit(" – ", 1)
                        title = parts[0].strip()
                        company = parts[1].strip()
                    elif " - " in title_text:
                        parts = title_text.rsplit(" - ", 1)
                        title = parts[0].strip()
                        company = parts[1].strip()
                        
                    # Deduce remote status
                    desc_lower = description.lower()
                    title_lower = title.lower()
                    
                    remote = False
                    if "remote" in title_lower or "home office" in title_lower or "home-office" in title_lower:
                        remote = True
                    elif "remote" in desc_lower or "home office" in desc_lower or "work from home" in desc_lower or "mobiles arbeiten" in desc_lower:
                        remote = True
                        
                    location = "Berlin"
                    if remote:
                        if "hybrid" in desc_lower or "hybrid" in title_lower:
                            location = "Berlin (Hybrid)"
                        else:
                            location = "Remote / Berlin"
                            
                    # Simple parsing to check if it's a tech role
                    combined = f"{title_lower} {desc_lower}"
                    is_tech = any(keyword in combined for keyword in ['data', 'engineer', 'scientist', 'developer', 'analyst', 'ml', 'ai', 'cloud', 'devops', 'software', 'programming', 'code'])
                    if not is_tech:
                        continue
                        
                    # Generate a unique job ID from link
                    job_hash = hashlib.sha256(link.encode()).hexdigest()[:12]
                    
                    jobs.append({
                        'job_id': f"bsj_{job_hash}",
                        'title': title,
                        'company': company,
                        'location': location,
                        'url': link,
                        'description': description,
                        'remote': remote,
                        'tags': 'Startup,Berlin',
                        'job_types': 'full_time',
                        'source': 'berlin_startups'
                    })
            except Exception as e:
                print(f"Warning: Failed to fetch/parse feed {url}: {str(e)}")
                
        print(f"Fetched {len(jobs)} total tech/data jobs from Berlin Startup Jobs.")
        return {'data': jobs}