import os
import re
import pandas as pd
from datetime import datetime, timezone
from processing.fetchers import BAFetcher
from processing.company_normalize import normalize_company


def _looks_remote(*values):
    haystack = " ".join(str(v).lower() for v in values if v)
    return bool(
        re.search(
            r"\b(remote|hybrid|home[- ]?office|work from home|mobiles arbeiten|telearbeit)\b",
            haystack,
        )
    )


def lambda_handler(event, context):
    """
    Fetch jobs from the public BA Jobsuche API (static public API key, no OAuth)
    and store them in the Bronze bucket.
    """
    bucket = os.environ.get("BRONZE_BUCKET")
    is_local = os.environ.get("LOCAL_RUN") == "true"

    try:
        if not bucket and not is_local:
            raise ValueError("BRONZE_BUCKET environment variable is not set.")
        fetcher = BAFetcher()
        queries = [
            "Data Engineer",
            "Data Scientist",
            "Data Analyst",
            "Business Intelligence",
            "Machine Learning",
            "MLOps",
            "AI Engineer",
            "Artificial Intelligence",
            "Forward Deployed Engineer",
            "Deep Learning",
            "DevOps",
            "Software Engineer",
            "Platform Engineer",
            "Cloud Architect",
            "SRE",
            "Werkstudent Data",
            "Werkstudent Software",
            "Praktikum Data",
            "Praktikum Software",
            "Junior Data Engineer",
            "Junior Data Scientist",
            "Junior Software Engineer",
            "Trainee IT",
        ]
        all_jobs = []
        seen_ids = set()
        for query in queries:
            result = fetcher.fetch_jobs(query=query)
            for job in result["stellenangebote"]:
                if job["refnr"] not in seen_ids:
                    seen_ids.add(job["refnr"])
                    all_jobs.append(job)
            print(f"Query '{query}': {len(result['stellenangebote'])} jobs fetched")

        df = pd.DataFrame(all_jobs)

        if df.empty:
            print("No jobs found from BA API.")
            return {"statusCode": 204, "body": "No jobs found from BA API."}

        # --- Normalize columns ---

        # Flatten nested arbeitsort dict into separate columns
        if "arbeitsort" in df.columns:
            arbeitsort_df = df["arbeitsort"].apply(lambda x: x if isinstance(x, dict) else {}).apply(pd.Series)
            df["location"] = arbeitsort_df.get("ort", pd.Series([""] * len(df)))
            df["zip_code"] = arbeitsort_df.get("plz", pd.Series([""] * len(df)))
            df["state"] = arbeitsort_df.get("region", pd.Series([""] * len(df)))
            df.drop(columns=["arbeitsort"], inplace=True)

        # Rename German field names to unified English schema
        df.rename(
            columns={
                "refnr": "job_id",
                "titel": "title",
                "arbeitgeber": "company",
                "eintrittsdatum": "start_date_raw",
                "modifikationsdatum": "modified_at",
                "modifikationsTimestamp": "modified_at",
            },
            inplace=True,
        )

        # Construct apply URL from job_id (refnr)
        df["url"] = "https://www.arbeitsagentur.de/jobsuche/jobdetail/" + df["job_id"].astype(str)

        if "company" in df.columns:
            df["company"] = df["company"].apply(lambda c: normalize_company(c) or "")

        title_col = df["title"] if "title" in df.columns else pd.Series([""] * len(df))
        loc_col = df["location"] if "location" in df.columns else pd.Series([""] * len(df))
        df["remote"] = [
            _looks_remote(t, loc, "")
            for t, loc in zip(title_col, loc_col)
        ]

        # Add source and ingestion timestamp
        df["source"] = "ba_api"
        df["ingested_at"] = datetime.now(timezone.utc).isoformat()

        # Partition by date
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = f"s3://{bucket}/ba_api/ingested_at={date_str}/jobs.parquet"

        from processing.utils import save_parquet
        save_parquet(df, path, "ba_api")
        print(f"Successfully ingested {len(df)} jobs from BA API.")
        return {"statusCode": 200, "body": f"Successfully ingested {len(df)} jobs from BA."}

    except Exception as e:
        # Re-raise so EventBridge marks the invocation failed and the DLQ/alarms fire.
        print(f"BA API ingestion failed: {str(e)}")
        raise
