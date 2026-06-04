import os
import json
import requests
import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone
import boto3


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


from processing.eu_filter import is_in_europe


def normalize_job_item(item, source_name):
    """
    Normalize varying Apify LinkedIn/Indeed scraper formats to unified schema.
    """
    # 1. Title
    title = item.get("title") or item.get("positionName") or item.get("jobTitle") or ""

    # 2. Company
    company_val = item.get("companyName") or item.get("company") or item.get("company_name")
    if isinstance(company_val, dict):
        company = company_val.get("name") or ""
    elif not company_val and isinstance(item.get("employer"), dict):
        company = item.get("employer", {}).get("name") or ""
    else:
        company = company_val or ""

    # 3. Location
    location_val = item.get("location") or item.get("locationName") or ""
    if isinstance(location_val, dict):
        location = location_val.get("cityName") or location_val.get("city") or location_val.get("countryName") or ""
    else:
        location = location_val or ""

    # Strict European Union filtering
    if not is_in_europe(location_str=location, title_str=title, item=item):
        return None

    # 4. URL
    url = item.get("url") or item.get("link") or item.get("jobUrl") or item.get("applyUrl") or ""

    # 5. Description
    desc_val = item.get("description") or item.get("descriptionHtml") or item.get("descriptionText") or ""
    if isinstance(desc_val, dict):
        description = desc_val.get("text") or desc_val.get("html") or ""
    else:
        description = desc_val or ""

    # Try to extract a clean job ID
    job_id = item.get("id") or item.get("jobId") or item.get("job_id")
    if not job_id and url:
        # Fallback to hash of URL if ID is not explicitly present
        import hashlib

        job_id = f"apify_{hashlib.sha256(url.encode()).hexdigest()[:12]}"
    elif job_id:
        job_id = f"apify_{job_id}"
    else:
        return None

    # Work out if remote
    remote_val = item.get("isRemote") or item.get("remote") or False
    remote = False
    if str(remote_val).lower() in ("true", "yes", "1"):
        remote = True
    elif any(
        k in str(title).lower() or k in str(location).lower()
        for k in ["remote", "telearbeit", "home office", "home-office"]
    ):
        remote = True

    tags = item.get("tags") or item.get("industries") or item.get("categories") or ""
    if isinstance(tags, list):
        tags = ",".join(tags)

    # Build clean standardized item
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
        "published_at": str(item.get("postedAt") or item.get("publishedAt") or item.get("date") or ""),
    }


def lambda_handler(event, context):
    """
    Fetch the latest completed datasets from Apify and save them in the Bronze bucket.
    """
    bucket = os.environ.get("BRONZE_BUCKET")
    if not bucket:
        print("ERROR: BRONZE_BUCKET environment variable is not set.")
        return {"statusCode": 500, "body": "BRONZE_BUCKET is required."}

    # Fetch token and task dictionary
    token, tasks = get_apify_credentials()
    if not token or not tasks:
        print("WARNING: Apify credentials or task configurations are not defined. Skipping run.")
        return {
            "statusCode": 200,
            "body": "Skipped. Apify credentials not configured (setup /dataforge/dev/apify_credentials in SSM).",
        }

    total_ingested = 0
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for source_name, task_id in tasks.items():
        print(f"Processing Apify source '{source_name}' (Task ID: {task_id})...")
        try:
            # 1. Trigger a new run of the task asynchronously (runs in the background on Apify)
            trigger_url = f"https://api.apify.com/v2/actor-tasks/{task_id}/runs"
            try:
                trigger_res = requests.post(trigger_url, params={"token": token}, timeout=10)
                if trigger_res.status_code == 201:
                    new_run_id = trigger_res.json().get("data", {}).get("id")
                    print(f"Triggered new run {new_run_id} for task {task_id}.")
                else:
                    print(
                        f"WARNING: Failed to trigger run for task {task_id}: {trigger_res.status_code} - {trigger_res.text}"
                    )
            except Exception as te:
                print(f"WARNING: Failed to trigger run for task {task_id}: {str(te)}")

            # 2. Fetch the latest succeeded run of the task (picks up the previous day's run)
            runs_url = f"https://api.apify.com/v2/actor-tasks/{task_id}/runs"
            params = {"token": token, "limit": 10, "desc": "true"}
            res = requests.get(runs_url, params=params, timeout=15)
            res.raise_for_status()
            runs = res.json().get("data", {}).get("items", [])

            # Find the latest succeeded run
            succeeded_run = None
            for run in runs:
                if run.get("status") == "SUCCEEDED":
                    succeeded_run = run
                    break

            if not succeeded_run:
                print(f"No completed successful runs found for task {task_id}.")
                continue

            dataset_id = succeeded_run.get("defaultDatasetId")
            run_id = succeeded_run.get("id")
            finished_at = succeeded_run.get("finishedAt", date_str)
            print(f"Found latest succeeded run {run_id} (dataset {dataset_id}) completed at {finished_at}.")

            # 3. Get dataset items
            items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
            items_params = {"token": token, "clean": "true"}
            items_res = requests.get(items_url, params=items_params, timeout=20)
            items_res.raise_for_status()
            raw_items = items_res.json()

            print(f"Retrieved {len(raw_items)} items from dataset.")

            # 3. Normalize items
            normalized_items = []
            for item in raw_items:
                normalized = normalize_job_item(item, source_name)
                if normalized and normalized["title"] and normalized["company"]:
                    normalized_items.append(normalized)

            if not normalized_items:
                print(f"No valid jobs found after normalization for source '{source_name}'.")
                continue

            df = pd.DataFrame(normalized_items)
            df["ingested_at"] = datetime.now(timezone.utc).isoformat()

            # 4. Save to S3 Bronze
            path = f"s3://{bucket}/apify_{source_name}/ingested_at={date_str}/jobs.parquet"
            wr.s3.to_parquet(df=df, path=path, index=False)
            print(f"Successfully saved {len(df)} normalized jobs to: {path}")
            total_ingested += len(df)

        except Exception as e:
            print(f"ERROR: Failed processing task {task_id} ({source_name}): {str(e)}")

    return {
        "statusCode": 200,
        "body": f"Successfully processed Apify ingestion. Total jobs ingested: {total_ingested}.",
    }
