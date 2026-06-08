import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone
import boto3

# Recommended Apify actor (proven in DataForge: ~300 results/run, ~10–20s runtime).
RECOMMENDED_INDEED_ACTOR = "valig/indeed-jobs-scraper"


def get_apify_credentials():
    """
    Retrieve Apify API token and tasks from environment variables or SSM Parameter Store.
    Returns:
        tuple: (apify_token, tasks_dict) or (None, None)
    """
    # 1. Try environment variables first
    token = os.environ.get("APIFY_TOKEN")
    tasks_json = os.environ.get("APIFY_TASKS")

    if token and tasks_json:
        try:
            return token, json.loads(tasks_json)
        except json.JSONDecodeError:
            # Fallback if tasks is a comma-separated list of IDs
            tasks_list = [t.strip() for t in tasks_json.split(",") if t.strip()]
            tasks_dict = {f"task_{i}": task_id for i, task_id in enumerate(tasks_list)}
            return token, tasks_dict

    # 2. Try SSM Parameter Store
    ssm_name = os.environ.get("SSM_APIFY_PARAMETER_NAME", "/dataforge/dev/apify_credentials")
    try:
        ssm = boto3.client("ssm", region_name="eu-central-1")
        param = ssm.get_parameter(Name=ssm_name, WithDecryption=True)
        config = json.loads(param["Parameter"]["Value"])
        return config.get("apify_token"), config.get("tasks", {})
    except Exception as e:
        print(f"SSM Fetch skipped or failed: {str(e)}")

    return None, None


def select_indeed_tasks(tasks):
    """Keep Indeed task(s) only. Supports keys 'indeed' or 'indeed_<variant>'."""
    return {k: v for k, v in tasks.items() if k == "indeed" or k.startswith("indeed_")}


def wait_for_run_completion(token, run_id, max_wait_seconds=240, poll_interval=10):
    """
    Poll an Apify actor run until it finishes or times out.
    valig/indeed-jobs-scraper typically completes in under 20 seconds.
    """
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    deadline = time.time() + max_wait_seconds

    while time.time() < deadline:
        res = requests.get(url, params={"token": token}, timeout=15)
        res.raise_for_status()
        run = res.json().get("data", {})
        status = run.get("status")
        if status == "SUCCEEDED":
            return run
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"Run {run_id} ended with status {status}.")
            return None
        time.sleep(poll_interval)

    print(f"Run {run_id} did not finish within {max_wait_seconds}s.")
    return None


def fetch_latest_succeeded_run(token, task_id):
    """Fallback: return the most recent successful run for a saved task."""
    runs_url = f"https://api.apify.com/v2/actor-tasks/{task_id}/runs"
    params = {"token": token, "limit": 10, "desc": "true"}
    res = requests.get(runs_url, params=params, timeout=15)
    res.raise_for_status()
    for run in res.json().get("data", {}).get("items", []):
        if run.get("status") == "SUCCEEDED":
            return run
    return None


def resolve_run_for_task(token, task_id):
    """
    Trigger a fresh task run, wait for completion, then fall back to the last succeeded run.
    """
    max_wait = int(os.environ.get("APIFY_RUN_MAX_WAIT_SECONDS", "240"))
    triggered_run_id = None

    trigger_url = f"https://api.apify.com/v2/actor-tasks/{task_id}/runs"
    try:
        trigger_res = requests.post(trigger_url, params={"token": token}, timeout=10)
        if trigger_res.status_code == 201:
            triggered_run_id = trigger_res.json().get("data", {}).get("id")
            print(f"Triggered new run {triggered_run_id} for task {task_id}.")
            completed = wait_for_run_completion(token, triggered_run_id, max_wait_seconds=max_wait)
            if completed:
                return completed, "triggered"
            print(f"Triggered run {triggered_run_id} did not succeed; falling back to latest succeeded run.")
        else:
            print(
                f"WARNING: Failed to trigger run for task {task_id}: "
                f"{trigger_res.status_code} - {trigger_res.text}"
            )
    except Exception as te:
        print(f"WARNING: Failed to trigger run for task {task_id}: {str(te)}")

    fallback = fetch_latest_succeeded_run(token, task_id)
    if fallback:
        if triggered_run_id and fallback.get("id") == triggered_run_id:
            return None, "failed"
        return fallback, "fallback"
    return None, "none"


def fetch_dataset_items(token, dataset_id):
    items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    items_res = requests.get(items_url, params={"token": token, "clean": "true"}, timeout=60)
    items_res.raise_for_status()
    return items_res.json()


from processing.europe_filter import is_in_europe


def _format_location_dict(location_dict):
    city = location_dict.get("cityName") or location_dict.get("city") or ""
    region = location_dict.get("admin1Code") or location_dict.get("state") or ""
    country = location_dict.get("countryName") or location_dict.get("countryCode") or ""
    parts = [p for p in (city, region, country) if p]
    return ", ".join(str(p) for p in parts)


def normalize_job_item(item, source_name):
    """
    Normalize Apify Indeed scraper formats to unified schema.
    Primary target: valig/indeed-jobs-scraper (nested location/employer, jobKey, jobUrl).
    """

    # 1. Title
    title = item.get("title") or item.get("positionName") or item.get("jobTitle") or ""

    # 2. Company — valig uses employer.name; other actors use companyName/company
    company_val = (
        item.get("companyName")
        or item.get("company")
        or item.get("company_name")
        or item.get("employerName")
    )
    if isinstance(company_val, dict):
        company = company_val.get("name") or ""
    elif not company_val and isinstance(item.get("employer"), dict):
        company = item.get("employer", {}).get("name") or ""
    elif not company_val and isinstance(item.get("employer"), str):
        company = item.get("employer") or ""
    else:
        company = company_val or ""

    # 3. Location — valig uses nested location dict or formattedLocation
    location_val = (
        item.get("formattedLocation")
        or item.get("location")
        or item.get("locationName")
        or item.get("country")
        or item.get("countryName")
        or ""
    )
    location_map = item.get("locationMap")
    if not location_val and isinstance(location_map, dict) and len(location_map) > 0:
        location_val = list(location_map.keys())[0]

    if isinstance(location_val, dict):
        location = _format_location_dict(location_val)
    elif isinstance(location_val, list) and len(location_val) > 0:
        first_loc = location_val[0]
        if isinstance(first_loc, dict):
            location = _format_location_dict(first_loc)
        else:
            location = str(first_loc)
    else:
        location = location_val or ""

    # Strict European filtering (pass full item for nested location.countryCode)
    if not is_in_europe(location_str=location, title_str=title, item=item):
        return None

    # 4. URL — valig uses jobUrl
    url = (
        item.get("url")
        or item.get("link")
        or item.get("jobUrl")
        or item.get("applyUrl")
        or item.get("urlToJob")
        or item.get("url_to_job")
        or ""
    )

    # 5. Description — valig uses descriptionText
    desc_val = item.get("description") or item.get("descriptionText") or item.get("descriptionHtml") or ""
    if isinstance(desc_val, dict):
        description = desc_val.get("text") or desc_val.get("html") or ""
    else:
        description = desc_val or ""

    # Job ID — valig uses jobKey
    job_id = item.get("jobKey") or item.get("id") or item.get("jobId") or item.get("job_id")
    if not job_id and url:
        import hashlib

        job_id = f"apify_{hashlib.sha256(url.encode()).hexdigest()[:12]}"
    elif job_id:
        job_id = f"apify_{job_id}"
    else:
        return None

    # Remote flag
    remote_val = item.get("isRemote") or item.get("remote") or False
    remote = False
    if str(remote_val).lower() in ("true", "yes", "1"):
        remote = True
    elif any(
        k in str(title).lower() or k in str(location).lower()
        for k in ["remote", "telearbeit", "home office", "home-office"]
    ):
        remote = True

    tags = item.get("tags") or item.get("skills") or item.get("industries") or item.get("categories") or ""
    if isinstance(tags, list):
        tags = ",".join(str(t) for t in tags)

    published = (
        item.get("datePublished")
        or item.get("postedAt")
        or item.get("publishedAt")
        or item.get("date")
        or item.get("scrapedAt")
        or ""
    )

    return {
        "job_id": str(job_id),
        "title": str(title).strip(),
        "company": str(company).strip(),
        "location": str(location).strip(),
        "url": str(url).strip(),
        "description": str(description).strip(),
        "remote": bool(remote),
        "tags": str(tags),
        "job_types": str(item.get("jobType") or item.get("employmentType") or "full_time"),
        "source": source_name,
        "published_at": str(published),
    }


def fetch_normalized_jobs_for_task(token, task_key, task_id, date_str):
    """Trigger/wait for an Apify task run and return normalized job dicts."""
    source_name = "indeed"
    print(f"Processing Apify source '{task_key}' -> {source_name} (task {task_id}, actor: {RECOMMENDED_INDEED_ACTOR})...")

    succeeded_run, run_mode = resolve_run_for_task(token, task_id)
    if not succeeded_run:
        print(f"No completed successful runs found for task {task_id}.")
        return []

    dataset_id = succeeded_run.get("defaultDatasetId")
    run_id = succeeded_run.get("id")
    finished_at = succeeded_run.get("finishedAt", date_str)
    print(f"Using {run_mode} run {run_id} (dataset {dataset_id}) completed at {finished_at}.")

    raw_items = fetch_dataset_items(token, dataset_id)
    print(f"Retrieved {len(raw_items)} raw items from dataset.")

    normalized_items = []
    for item in raw_items:
        normalized = normalize_job_item(item, source_name)
        if normalized and normalized["title"] and normalized["company"]:
            normalized_items.append(normalized)

    filtered_out = len(raw_items) - len(normalized_items)
    print(
        f"Normalization: {len(normalized_items)} kept, {filtered_out} dropped "
        f"(EU filter / missing title-company)."
    )
    return normalized_items


def lambda_handler(event, context):
    """
    Fetch Indeed scrape results from Apify (valig/indeed-jobs-scraper task) and save Bronze parquet.
    """
    bucket = os.environ.get("BRONZE_BUCKET")
    is_local = os.environ.get("LOCAL_RUN") == "true"
    if not bucket and not is_local:
        print("ERROR: BRONZE_BUCKET environment variable is not set.")
        return {"statusCode": 500, "body": "BRONZE_BUCKET is required."}

    if os.environ.get("DISABLE_APIFY") == "true":
        print("INFO: Apify fetching is disabled via environment variable.")
        return {
            "statusCode": 200,
            "body": "Skipped. Apify fetching is disabled via DISABLE_APIFY environment variable.",
        }

    token, tasks = get_apify_credentials()
    if not token or not tasks:
        print("WARNING: Apify credentials or task configurations are not defined. Skipping run.")
        return {
            "statusCode": 200,
            "body": "Skipped. Apify credentials not configured (setup /dataforge/dev/apify_credentials in SSM).",
        }

    tasks = select_indeed_tasks(tasks)
    if not tasks:
        print("WARNING: No indeed task configured in Apify tasks. Skipping run.")
        return {
            "statusCode": 200,
            "body": "Skipped. No indeed task configured in Apify credentials.",
        }

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_jobs = []
    seen_ids = set()

    for task_key, task_id in tasks.items():
        try:
            for job in fetch_normalized_jobs_for_task(token, task_key, task_id, date_str):
                if job["job_id"] not in seen_ids:
                    seen_ids.add(job["job_id"])
                    all_jobs.append(job)
        except Exception as e:
            print(f"ERROR: Failed processing task {task_id} ({task_key}): {str(e)}")

    if not all_jobs:
        return {
            "statusCode": 200,
            "body": f"No Indeed jobs ingested ({RECOMMENDED_INDEED_ACTOR}). Check Apify billing and task runs.",
        }

    df = pd.DataFrame(all_jobs)
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    path = f"s3://{bucket}/apify_indeed/ingested_at={date_str}/jobs.parquet"
    from processing.utils import save_parquet

    save_parquet(df, path, "apify_indeed")
    total_ingested = len(df)
    print(f"Successfully saved {total_ingested} deduplicated Indeed jobs to: {path}")

    return {
        "statusCode": 200,
        "body": (
            f"Successfully processed Apify ingestion ({RECOMMENDED_INDEED_ACTOR}). "
            f"Total jobs ingested: {total_ingested}."
        ),
    }
