import os
import json
import pandas as pd
import awswrangler as wr
import re
from processing.europe_filter import classify_region


def detect_is_english(row):
    title = str(row.get("title", "")).lower()
    description = str(row.get("description", "")).lower()
    text = title + " " + description

    english_words = re.findall(
        r"\b(the|and|for|with|you|our|your|we|are|team|role|experience|skills|requirements|responsibilities|software|engineer|developer|data|management|design|is)\b",
        text,
    )
    german_words = re.findall(
        r"\b(und|die|der|mit|wir|sie|für|eine|ist|sind|das|arbeitszeit|aufgaben|profil|wir bieten|qualifikation|erfahrung|kenntnisse|mitarbeit|bereich|stelle|stelleanzeige)\b",
        text,
    )

    en_count = len(english_words)
    de_count = len(german_words)

    if en_count == 0 and de_count == 0:
        en_title_keywords = re.search(
            r"\b(engineer|developer|manager|specialist|lead|analyst|designer|architect|coordinator|consultant|expert)\b",
            text,
        )
        return bool(en_title_keywords)

    return en_count >= de_count


def detect_language_requirement(row):
    is_english = bool(row.get("is_english", True))
    if not is_english:
        return "german_required"

    # If text is in English, scan for German requirements
    title = str(row.get("title", "")).lower()
    description = str(row.get("description", "")).lower()
    text = title + " " + description

    # German requirement pattern
    german_req_pattern = re.compile(
        r"\b(german\s+skills|german\s+language|fluent\s+german|speak\s+german|german\s+level|deutsch\s+sprechen|b2|c1|c2|deutschkenntnisse|deutsch\s+auf|knowledge\s+of\s+german)\b",
        re.IGNORECASE,
    )

    if german_req_pattern.search(text):
        return "german_required"
    return "english_only"


def detect_work_style(row):
    # Check if remote is marked
    is_remote_val = row.get("remote")
    is_remote = False
    if is_remote_val is True or str(is_remote_val).lower() == "true":
        is_remote = True

    title = str(row.get("title", "")).lower()
    description = str(row.get("description", "")).lower()
    text = title + " " + description

    # Check for hybrid keywords
    is_hybrid = False
    if any(
        k in text
        for k in ["hybrid", "home office", "home-office", "mobiles arbeiten", "days in office", "days a week in"]
    ):
        is_hybrid = True

    if is_hybrid:
        return "hybrid"
    elif is_remote:
        return "remote"
    else:
        return "onsite"


def lambda_handler(event, context):
    silver_path = os.environ.get("SILVER_PATH")
    gold_bucket = os.environ.get("GOLD_BUCKET")

    try:
        if not silver_path or not gold_bucket:
            raise ValueError("SILVER_PATH and GOLD_BUCKET environment variables must be set.")

        print("Reading Silver data...")
        active_path = f"{silver_path}is_current=True/"
        inactive_path = f"{silver_path}is_current=False/"
        
        dfs = []
        active_objects = []
        try:
            active_objects = wr.s3.list_objects(path=active_path)
        except Exception as e:
            print(f"Warning: Failed to list active path: {e}")
            
        inactive_objects = []
        try:
            inactive_objects = wr.s3.list_objects(path=inactive_path)
        except Exception as e:
            print(f"Warning: Failed to list inactive path: {e}")

        for f in active_objects:
            if f:
                try:
                    df_part = wr.s3.read_parquet(path=f)
                    if not df_part.empty:
                        df_part["is_current"] = True
                        dfs.append(df_part)
                except Exception as ex:
                    print(f"Warning: Failed to read active file {f}: {ex}")

        for f in inactive_objects:
            if f:
                try:
                    df_part = wr.s3.read_parquet(path=f)
                    if not df_part.empty:
                        df_part["is_current"] = False
                        dfs.append(df_part)
                except Exception as ex:
                    print(f"Warning: Failed to read inactive file {f}: {ex}")

        if not dfs:
            raise ValueError("No Silver data found in S3.")
            
        df = pd.concat(dfs, ignore_index=True)
        df["is_current"] = df["is_current"].astype(bool)
        
        # Standardize date column schemas to avoid any type incompatibilities in metrics/trend calculations
        for date_col in ["scd_start_date", "scd_end_date"]:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
                if df[date_col].dt.tz is None:
                    df[date_col] = df[date_col].dt.tz_localize("UTC")
                else:
                    df[date_col] = df[date_col].dt.tz_convert("UTC")

        # Automatically enrich tags with semantic categories
        def enrich_tags(row):
            title = str(row.get("title", "")).lower()
            tags = str(row.get("tags", "")).lower()
            description = str(row.get("description", "")).lower()
            combined = title + " " + tags + " " + description

            system_tags = []
            if any(
                re.search(pat, combined)
                for pat in [
                    r"\bdata scientist\b",
                    r"\bdata science\b",
                    r"\bmachine learning\b",
                    r"\bml\b",
                    r"\bmlops\b",
                    r"\bai engineer\b",
                    r"\bai developer\b",
                    r"\bai architect\b",
                    r"\bartificial intelligence\b",
                    r"\bgenerative ai\b",
                    r"\bgenai\b",
                    r"\bllm\b",
                    r"\bnlp\b",
                    r"\bcomputer vision\b",
                    r"\bdeep learning\b",
                    r"\bdeep-learning\b",
                    r"\bagentic\b",
                ]
            ):
                system_tags.append("AI / ML")
            if any(
                re.search(pat, combined)
                for pat in [
                    r"\bdata engineer\b",
                    r"\betl\b",
                    r"\belt\b",
                    r"\bdata pipeline\b",
                    r"\bdbt\b",
                    r"\bdatabricks\b",
                    r"\bsnowflake\b",
                ]
            ):
                system_tags.append("Data Engineering")
            if any(
                re.search(pat, combined)
                for pat in [
                    r"\bdevops\b",
                    r"\bplatform engineer\b",
                    r"\bcloud engineer\b",
                    r"\bsre\b",
                    r"\bsite reliability\b",
                    r"\bkubernetes\b",
                    r"\bterraform\b",
                    r"\baws\b",
                    r"\bazure\b",
                    r"\bgcp\b",
                    r"\bcloud architect\b",
                ]
            ):
                system_tags.append("Cloud / DevOps")
            if any(
                re.search(pat, combined)
                for pat in [
                    r"\bdata analyst\b",
                    r"\banalytics\b",
                    r"\bbusiness intelligence\b",
                    r"\bbi\b",
                    r"\btableau\b",
                    r"\bpower bi\b",
                ]
            ):
                system_tags.append("Analytics / BI")
            if any(re.search(pat, combined) for pat in [r"\bforward deployed\b", r"\bfde\b"]):
                system_tags.append("Forward Deployed")

            # Experience / role type tags
            # Junior / Entry Level
            is_junior = any(
                re.search(pat, combined)
                for pat in [
                    r"\bjunior\b",
                    r"\bentry-level\b",
                    r"\bentry level\b",
                    r"\bfresher\b",
                    r"\btrainee\b",
                    r"\bberufseinsteiger\b",
                    r"\babsolvent\b",
                    r"\bstarter\b",
                    r"\bbeginner\b",
                    r"\beinsteiger\b",
                ]
            )
            # Exclude junior tag if title contains senior/lead/director keywords
            is_senior_title = any(
                re.search(pat, title)
                for pat in [r"\bsenior\b", r"\blead\b", r"\bprincipal\b", r"\bdirector\b", r"\bhead\b"]
            )
            if is_junior and not is_senior_title:
                system_tags.append("Junior / Entry Level")

            # Working Student
            if any(
                re.search(pat, combined)
                for pat in [
                    r"\bwerkstudent\b",
                    r"\bwerkstudenten\b",
                    r"\bwerkstudententätigkeit\b",
                    r"\bworking student\b",
                    r"\bworking-student\b",
                ]
            ):
                system_tags.append("Working Student")

            # Internship
            if any(
                re.search(pat, combined)
                for pat in [
                    r"\binternship\b",
                    r"\bintern\b",
                    r"\bpraktikum\b",
                    r"\bpraktikant\b",
                    r"\bpraktikantin\b",
                    r"\bpraktikanten\b",
                ]
            ):
                system_tags.append("Internship")

            # Master Thesis
            if any(
                re.search(pat, combined)
                for pat in [
                    r"\bmaster thesis\b",
                    r"\bmaster-thesis\b",
                    r"\bmasterarbeit\b",
                    r"\babschlussarbeit\b",
                    r"\bbachelor thesis\b",
                    r"\bbachelorarbeit\b",
                    r"\bbachelor-thesis\b",
                ]
            ):
                system_tags.append("Master Thesis")

            original_tags = str(row.get("tags", ""))
            if original_tags and original_tags not in {"nan", "<NA>", "None", ""}:
                cleaned_original = [
                    t.strip()
                    for t in original_tags.split(",")
                    if t.strip() and t.strip() not in {"nan", "<NA>", "None", ""}
                ]
                return ",".join(system_tags + cleaned_original)
            else:
                return ",".join(system_tags)

        # Apply tag enrichment
        df["tags"] = df.apply(enrich_tags, axis=1)

        # Calculate is_english backend field
        df["is_english"] = df.apply(detect_is_english, axis=1)
        df["language_requirement"] = df.apply(detect_language_requirement, axis=1)
        df["work_style"] = df.apply(detect_work_style, axis=1)
        df["region"] = df.apply(
            lambda r: classify_region(
                location_str=r.get("location", ""),
                title_str=r.get("title", ""),
                description_str=r.get("description", ""),
                item=r
            ),
            axis=1
        )

        current = df[df["is_current"] == True].copy().reset_index(drop=True)
        print(f"Total active jobs: {len(current)}")

        # 1. All active jobs
        cols = [
            c
            for c in [
                "job_id",
                "title",
                "company",
                "location",
                "zip_code",
                "state",
                "source",
                "ats",
                "department",
                "scd_start_date",
                "remote",
                "url",
                "job_types",
                "tags",
                "description",
                "salary",
                "published_at",
                "start_date_raw",
                "modified_at",
                "ingested_at",
                "is_english",
                "work_style",
                "language_requirement",
                "region",
            ]
            if c in current.columns
        ]
        all_jobs = current[cols].copy()
        if "description" in all_jobs.columns:
            all_jobs["description"] = all_jobs["description"].fillna("").astype(str).str.slice(0, 300)
        all_jobs["date_added"] = pd.to_datetime(all_jobs["scd_start_date"]).dt.date.astype(str)
        all_jobs.drop(columns=["scd_start_date"], inplace=True)
        all_jobs.rename(columns={"url": "job_url", "remote": "is_remote"}, inplace=True)
        all_jobs["is_remote"] = all_jobs.get("is_remote", pd.Series(False, index=all_jobs.index)).apply(
            lambda x: True if str(x) == "True" else False
        )

        # 1b. Expired jobs (is_current=False)
        expired_raw = df[df["is_current"] == False].copy()
        exp_cols = [
            c
            for c in [
                "job_id",
                "title",
                "company",
                "location",
                "zip_code",
                "state",
                "source",
                "ats",
                "department",
                "scd_start_date",
                "scd_end_date",
                "remote",
                "url",
                "job_types",
                "tags",
                "salary",
                "published_at",
                "start_date_raw",
                "modified_at",
                "ingested_at",
                "is_english",
                "work_style",
                "language_requirement",
                "region",
            ]
            if c in expired_raw.columns
        ]
        expired_jobs = expired_raw[exp_cols].copy()
        expired_jobs["date_added"] = pd.to_datetime(expired_jobs["scd_start_date"]).dt.date.astype(str)
        expired_jobs["date_expired"] = pd.to_datetime(expired_jobs["scd_end_date"]).dt.date.astype(str)
        expired_jobs.drop(columns=["scd_start_date", "scd_end_date"], inplace=True)
        expired_jobs.rename(columns={"url": "job_url", "remote": "is_remote"}, inplace=True)
        expired_jobs["is_remote"] = expired_jobs.get("is_remote", pd.Series(False, index=expired_jobs.index)).apply(
            lambda x: True if str(x) == "True" else False
        )

        # 2. Jobs by source
        jobs_by_source = (
            current.groupby("source").size().reset_index(name="job_count").sort_values("job_count", ascending=False)
        )

        # 2b. Jobs by region
        jobs_by_region = (
            current.groupby("region").size().reset_index(name="job_count").sort_values("job_count", ascending=False)
        )

        # 3. Top locations — take first part before comma to clean "Berlin, Berlin, Germany" → "Berlin"
        current["location_clean"] = current["location"].str.split(",").str[0].str.strip()
        top_locations = (
            current[current["location_clean"].notna() & (current["location_clean"] != "")]
            .groupby("location_clean")
            .size()
            .reset_index(name="job_count")
            .sort_values("job_count", ascending=False)
            .head(20)
            .rename(columns={"location_clean": "location"})
        )

        # 4. Remote vs onsite (sources with an explicit remote signal)
        if "remote" in current.columns:
            remote_df = current[current["source"].isin(["arbeitnow", "direct"])].copy()
            remote_df["work_type"] = remote_df["remote"].apply(
                lambda x: "Remote" if x is True or str(x) == "True" else "On-site"
            )
            remote_vs_onsite = remote_df.groupby("work_type").size().reset_index(name="job_count")
        else:
            remote_vs_onsite = pd.DataFrame({"work_type": [], "job_count": []})

        # 5. Jobs trend — count only first appearance of each job_id (true new jobs)
        first_seen = df.sort_values("scd_start_date").drop_duplicates(subset="job_id", keep="first")
        first_seen["date"] = pd.to_datetime(first_seen["scd_start_date"]).dt.date.astype(str)
        jobs_trend = first_seen.groupby("date").size().reset_index(name="new_jobs").sort_values("date")

        # 6. Top companies
        top_companies = (
            current[current["company"].notna() & (current["company"] != "")]
            .groupby("company")
            .size()
            .reset_index(name="job_count")
            .sort_values("job_count", ascending=False)
            .head(20)
        )

        # 7. Active vs expired
        df["status"] = df["is_current"].apply(lambda x: "Active" if x else "Expired")
        active_vs_expired = df.groupby("status").size().reset_index(name="job_count")

        # 8. Top skills from tags (Arbeitnow) + title keywords (both sources)
        from collections import Counter
        import html

        def strip_html(text):
            """Remove HTML tags and decode entities."""
            text = re.sub(r"<[^>]+>", " ", str(text))
            return html.unescape(text)

        SKILL_KEYWORDS = [
            # Data
            "Python",
            "SQL",
            "Spark",
            "Kafka",
            "Airflow",
            "dbt",
            "Pandas",
            "Hadoop",
            "Hive",
            "Flink",
            "Databricks",
            "Snowflake",
            "BigQuery",
            # AI / ML
            "Machine Learning",
            "Deep Learning",
            "LLM",
            "NLP",
            "PyTorch",
            "TensorFlow",
            "Scikit",
            "MLflow",
            "Hugging Face",
            "OpenAI",
            "Generative AI",
            "Computer Vision",
            "RAG",
            "LangChain",
            # Cloud
            "AWS",
            "Azure",
            "GCP",
            "Kubernetes",
            "Docker",
            "Terraform",
            # BI / Analytics
            "Power BI",
            "Tableau",
            "Looker",
            "Excel",
            "Grafana",
            # Engineering
            "Java",
            "Scala",
            "Go",
            "TypeScript",
            "React",
            "FastAPI",
        ]
        skill_pattern = re.compile(r"\b(" + "|".join(re.escape(s) for s in SKILL_KEYWORDS) + r")\b", re.IGNORECASE)

        # Patterns for description-derived KPIs
        english_pattern = re.compile(
            r"\b(the|and|for|with|you|our|your|we are|we\'re|join|team|role|experience|skills|requirements|responsibilities)\b",
            re.IGNORECASE,
        )
        homeoffice_pattern = re.compile(
            r"\b(homeoffice|home.office|remote|hybrid|work from home|mobiles arbeiten)\b", re.IGNORECASE
        )
        benefits_pattern = re.compile(
            r"<h2[^>]*>\s*(benefits|benefits|vorteile|was wir bieten|was wir dir bieten|unser angebot)\s*</h2>",
            re.IGNORECASE,
        )

        skill_counter = Counter()
        english_count = 0
        homeoffice_desc_count = 0
        benefits_count = 0

        arbeitnow_jobs = current[current["source"] == "arbeitnow"]

        for _, row in current.iterrows():
            raw_desc = str(row.get("description", ""))
            plain_text = strip_html(raw_desc)
            combined = " ".join(
                filter(
                    None,
                    [
                        str(row.get("title", "")),
                        str(row.get("tags", "")),
                        plain_text[:500],
                    ],
                )
            )
            for match in skill_pattern.finditer(combined):
                skill_counter[match.group().title()] += 1

        for _, row in arbeitnow_jobs.iterrows():
            raw_desc = str(row.get("description", ""))
            plain_text = strip_html(raw_desc)
            if len(plain_text) > 100:
                en_matches = len(english_pattern.findall(plain_text[:1000]))
                total_words = len(plain_text[:1000].split())
                if total_words > 0 and (en_matches / total_words) > 0.04:
                    english_count += 1
            if homeoffice_pattern.search(raw_desc):
                homeoffice_desc_count += 1
            if benefits_pattern.search(raw_desc):
                benefits_count += 1

        top_skills = pd.DataFrame(skill_counter.most_common(20), columns=["skill", "job_count"])

        description_insights = pd.DataFrame(
            [
                {
                    "english_jobs": english_count,
                    "homeoffice_mentioned": homeoffice_desc_count,
                    "jobs_with_benefits": benefits_count,
                    "arbeitnow_total": len(arbeitnow_jobs),
                }
            ]
        )
        gold_base = f"s3://{gold_bucket}"
        wr.s3.to_csv(all_jobs, path=f"{gold_base}/all_jobs.csv", index=False, quoting=1)  # QUOTE_ALL
        wr.s3.to_csv(expired_jobs, path=f"{gold_base}/expired_jobs.csv", index=False, quoting=1)  # QUOTE_ALL
        wr.s3.to_csv(jobs_by_source, path=f"{gold_base}/jobs_by_source.csv", index=False)
        wr.s3.to_csv(jobs_by_region, path=f"{gold_base}/jobs_by_region.csv", index=False)
        wr.s3.to_csv(top_locations, path=f"{gold_base}/top_locations.csv", index=False)
        wr.s3.to_csv(remote_vs_onsite, path=f"{gold_base}/remote_vs_onsite.csv", index=False)
        wr.s3.to_csv(jobs_trend, path=f"{gold_base}/jobs_trend.csv", index=False)
        wr.s3.to_csv(top_companies, path=f"{gold_base}/top_companies.csv", index=False)
        wr.s3.to_csv(active_vs_expired, path=f"{gold_base}/active_vs_expired.csv", index=False)
        wr.s3.to_csv(top_skills, path=f"{gold_base}/top_skills.csv", index=False)
        wr.s3.to_csv(description_insights, path=f"{gold_base}/description_insights.csv", index=False)

        msg = f"Gold layer refreshed. Active jobs: {len(current)}, Files written: 11"
        print(msg)
        _trigger_github_redeploy(len(current))
        return {"statusCode": 200, "body": json.dumps({"message": msg})}

    except Exception as e:
        error_msg = f"Gold generation failed: {str(e)}"
        print(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}


def _trigger_github_redeploy(active_count):
    token = os.environ.get("GITHUB_TOKEN")
    owner = os.environ.get("GITHUB_OWNER", "ritesh8303")
    repo = os.environ.get("GITHUB_REPO", "dataforge")

    if not token:
        try:
            import boto3

            ssm = boto3.client("ssm", region_name="eu-central-1")
            param = ssm.get_parameter(Name="/dataforge/dev/github_token", WithDecryption=True)
            token = param["Parameter"]["Value"]
        except Exception as e:
            print(
                f"Skipping GitHub Pages trigger: GITHUB_TOKEN not found in environment or SSM Parameter Store ({str(e)})."
            )
            return

    url = f"https://api.github.com/repos/{owner}/{repo}/dispatches"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "DataForge-Lambda",
    }
    payload = {
        "event_type": "gold_data_updated",
        "client_payload": {
            "active_jobs": int(active_count),
            "timestamp": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        },
    }
    try:
        import requests

        res = requests.post(url, headers=headers, json=payload, timeout=10)
        if res.status_code == 204:
            print("Successfully triggered GitHub Actions workflow dispatch for Pages update.")
        else:
            print(f"WARNING: GitHub dispatch failed: {res.status_code} - {res.text}")
    except Exception as e:
        print(f"WARNING: Failed to request GitHub workflow dispatch: {str(e)}")
