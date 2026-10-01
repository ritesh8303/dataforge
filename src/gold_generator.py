import os
import json
from pathlib import Path

import pandas as pd
import awswrangler as wr
import re
from processing.data_quality import validate_region_taxonomy
from processing.europe_filter import COUNTRY_MAPPING, classify_region, normalize_region_bucket
from processing.company_normalize import normalize_company
from processing.data_quality import compute_quality_metrics, REMOVED_SOURCES, VALID_SOURCES

_EN_TITLE_ROLE_RE = re.compile(
    r"\b(engineer|developer|manager|specialist|lead|analyst|designer|architect|"
    r"coordinator|consultant|expert|scientist)\b",
    re.IGNORECASE,
)
_GERMAN_DESC_RE = re.compile(
    r"\b(und|der|die|das|wir|sie|für|aufgaben|qualifikation|erfahrung|kenntnisse|"
    r"stellenanzeige|bewerbung)\b",
    re.IGNORECASE,
)
_GERMAN_REQ_RE = re.compile(
    r"\b(german\s+skills|german\s+language|fluent\s+german|speak\s+german|"
    r"german\s+level|deutsch\s+sprechen|deutschkenntnisse|deutsch\s+auf|"
    r"knowledge\s+of\s+german|muttersprache\s+deutsch)\b",
    re.IGNORECASE,
)


def detect_is_english(row):
    """English if title contains English tech role keywords (m/w/d alone is not disqualifying)."""
    title = str(row.get("title", ""))
    return bool(_EN_TITLE_ROLE_RE.search(title))


def detect_language_requirement(row):
    title = str(row.get("title", "")).lower()
    description = str(row.get("description", "")).lower()
    text = f"{title} {description}"

    is_en = bool(row.get("is_english", False))
    has_german_desc = bool(_GERMAN_DESC_RE.search(description)) if description.strip() else False

    if _GERMAN_REQ_RE.search(text):
        return "german_required"
    if is_en and has_german_desc:
        return "bilingual"
    if is_en:
        return "english_only"
    return "german_required"


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

        # Exclude removed sources from historical Silver data
        if "source" in df.columns:
            before = len(df)
            df = df[~df["source"].isin(REMOVED_SOURCES)].copy()
            df = df[df["source"].isin(VALID_SOURCES)].copy()
            dropped = before - len(df)
            if dropped:
                print(f"Filtered {dropped} rows with removed/invalid sources from Silver.")

        if "company" in df.columns:
            df["company"] = df["company"].apply(lambda c: normalize_company(c) or "")

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
            lambda r: normalize_region_bucket(
                classify_region(
                    location_str=r.get("location", ""),
                    title_str=r.get("title", ""),
                    description_str=r.get("description", ""),
                    item=r,
                )
            ),
            axis=1,
        )

        current = df[df["is_current"] == True].copy().reset_index(drop=True)
        if "company" in current.columns:
            invalid_company = current["company"].isna() | (current["company"].astype(str).str.strip() == "")
            if invalid_company.any():
                print(f"Dropping {invalid_company.sum()} active jobs with invalid company after normalization.")
                current = current[~invalid_company].copy().reset_index(drop=True)
        lakehouse_active = len(current)
        print(f"Total active jobs (lakehouse): {lakehouse_active}")

        # 1. Product board — enrich labels, then hard-gate to EU × data/AI × early career
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
                "is_tech",
                "field_rule",
                "ai_field_rule",
                "seniority_rule",
                "employment_type_rule",
                "source_attribution",
                "dedup_key",
            ]
            if c in current.columns
        ]
        all_jobs = current[cols].copy()
        if "description" in all_jobs.columns:
            # Keep enough text for experience/seniority classification; truncate for board after gate.
            all_jobs["description"] = all_jobs["description"].fillna("").astype(str).str.slice(0, 1800)
        all_jobs["date_added"] = pd.to_datetime(all_jobs["scd_start_date"]).dt.date.astype(str)
        all_jobs.drop(columns=["scd_start_date"], inplace=True)
        all_jobs.rename(columns={"url": "job_url", "remote": "is_remote"}, inplace=True)
        all_jobs["is_remote"] = all_jobs.get("is_remote", pd.Series(False, index=all_jobs.index)).apply(
            lambda x: True if str(x) == "True" else False
        )

        from processing.audience_gate import enrich_jobs_with_audience
        from enrichment.ingest_review import enqueue_ingest_review

        records = all_jobs.to_dict(orient="records")

        # Merge prior AI enrichment so audience gate is AI-first (not rule-first).
        enrich_key = os.environ.get("ENRICHMENT_OUTPUT_KEY", "ai_job_enrichment.csv")
        if gold_bucket:
            try:
                enrich_df = wr.s3.read_csv(f"s3://{gold_bucket}/{enrich_key}")
                if not enrich_df.empty and "job_id" in enrich_df.columns:
                    emap = enrich_df.set_index("job_id").to_dict(orient="index")
                    for rec in records:
                        jid = str(rec.get("job_id") or "")
                        if jid in emap:
                            for k, v in emap[jid].items():
                                if v is not None and str(v).strip() not in {"", "nan"}:
                                    rec[k] = v
                    print(f"Merged AI enrichment for {sum(1 for r in records if r.get('ai_field'))} jobs")
            except Exception as exc:
                print(f"No prior AI enrichment merge ({exc})")
        else:
            local_enrich = Path("data/gold") / enrich_key
            if local_enrich.exists():
                try:
                    enrich_df = pd.read_csv(local_enrich)
                    if not enrich_df.empty and "job_id" in enrich_df.columns:
                        emap = enrich_df.set_index("job_id").to_dict(orient="index")
                        for rec in records:
                            jid = str(rec.get("job_id") or "")
                            if jid in emap:
                                for k, v in emap[jid].items():
                                    if v is not None and str(v).strip() not in {"", "nan"}:
                                        rec[k] = v
                except Exception as exc:
                    print(f"Local enrichment merge failed: {exc}")

        enriched_all = enrich_jobs_with_audience(records)
        try:
            from enrichment.canonical_title import attach_canonical_title

            enriched_all = [attach_canonical_title(r) for r in enriched_all]
        except Exception as exc:
            print(f"canonical_title skipped: {exc}")
        early_career = [r for r in enriched_all if r.get("audience_accept")]
        uncertain = [r for r in enriched_all if r.get("audience_uncertain")]
        if uncertain:
            dest = enqueue_ingest_review(uncertain)
            if dest:
                print(f"Queued {len(uncertain)} ambiguous jobs for ingest review -> {dest}")
            if os.environ.get("INGEST_REVIEW_AGENT", "true").lower() != "false":
                try:
                    from agent.ingest_review_agent import run_ingest_review_agent

                    summary = run_ingest_review_agent(
                        use_llm=os.environ.get("INGEST_REVIEW_USE_LLM", "true").lower()
                        in {"1", "true", "yes"},
                        max_llm=int(os.environ.get("INGEST_REVIEW_MAX_LLM", "40")),
                    )
                    print(
                        f"Ingest review agent: decided {summary.get('decided')} "
                        f"(accept={summary.get('counts', {}).get('accept', 0)}, "
                        f"reject={summary.get('counts', {}).get('reject', 0)}, "
                        f"methods={summary.get('methods')}) -> {summary.get('decisions_path')}"
                    )
                    enriched_all = enrich_jobs_with_audience(records)
                    try:
                        from enrichment.canonical_title import attach_canonical_title as _canon

                        enriched_all = [_canon(r) for r in enriched_all]
                    except Exception:
                        pass
                    early_career = [r for r in enriched_all if r.get("audience_accept")]
                    uncertain = [r for r in enriched_all if r.get("audience_uncertain")]
                except Exception as exc:
                    print(f"Ingest review agent skipped: {exc}")
        print(
            f"Audience gate: {len(early_career)} product jobs of {len(enriched_all)} lakehouse "
            f"(uncertain still: {len(uncertain)})"
        )
        # Jobs board publishes the full active lakehouse with audience flags.
        # Match/dashboard product KPIs keep using audience_accept=True rows.
        all_jobs = pd.DataFrame(enriched_all)
        if "description" in all_jobs.columns:
            all_jobs["description"] = all_jobs["description"].fillna("").astype(str).str.slice(0, 300)
        for col in (
            "ai_field",
            "ai_seniority",
            "employment_type",
            "ai_entry_level",
            "ai_english_ok",
            "audience_accept",
            "audience_eu",
            "audience_data_ai",
            "audience_seniority",
            "audience_uncertain",
            "field",
            "seniority",
        ):
            if col not in all_jobs.columns and enriched_all:
                all_jobs[col] = [a.get(col) for a in enriched_all]
        product_ids = set(str(r.get("job_id")) for r in early_career)
        product = current[current["job_id"].astype(str).isin(product_ids)].copy().reset_index(drop=True)
        if not len(product) and early_career:
            product = pd.DataFrame(early_career)
        print(
            f"Published lakehouse jobs: {len(all_jobs)} "
            f"(product/audience: {len(early_career)}, lakehouse active: {lakehouse_active})"
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
                "is_tech",
                "field_rule",
                "ai_field_rule",
                "source_attribution",
                "dedup_key",
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

        # 2–8. Dashboard aggregates over the full active board
        jobs_by_source = (
            product.groupby("source").size().reset_index(name="job_count").sort_values("job_count", ascending=False)
        )

        # 2b. Jobs by region (geographic only — never work-style "Remote")
        if len(product):
            validate_region_taxonomy(product["region"].unique())
        jobs_by_region = (
            product.groupby("region").size().reset_index(name="job_count").sort_values("job_count", ascending=False)
        )

        # 3. Top locations — take first part before comma to clean "Berlin, Berlin, Germany" → "Berlin"
        def clean_location_label(value):
            # Strip NUTS/region codes in parentheses, e.g. "MT (MT001)" → "MT"
            text = re.sub(r"\s*\([^)]*\)", "", str(value)).strip()
            # Map bare ISO country codes to country names, e.g. "MT" → "Malta"
            if len(text) <= 3:
                mapped = COUNTRY_MAPPING.get(text.lower())
                if mapped:
                    return mapped
            return text

        product["location_clean"] = (
            product["location"].astype(str).str.split(",").str[0].str.strip().apply(clean_location_label)
            if len(product)
            else pd.Series(dtype=str)
        )
        top_locations = (
            product[product["location_clean"].notna() & (product["location_clean"] != "")]
            .groupby("location_clean")
            .size()
            .reset_index(name="job_count")
            .sort_values("job_count", ascending=False)
            .head(20)
            .rename(columns={"location_clean": "location"})
        )

        # 4. Remote / hybrid / on-site — product board via work_style
        work_style_labels = {"remote": "Remote", "hybrid": "Hybrid", "onsite": "On-site"}
        if "work_style" in product.columns and len(product):
            remote_vs_onsite = (
                product.groupby("work_style")
                .size()
                .reset_index(name="job_count")
                .rename(columns={"work_style": "work_type"})
            )
            remote_vs_onsite["work_type"] = remote_vs_onsite["work_type"].map(
                lambda x: work_style_labels.get(str(x).lower(), str(x).title())
            )
        else:
            remote_vs_onsite = pd.DataFrame({"work_type": [], "job_count": []})

        # 5. Jobs trend — first appearance of product job_ids only
        product_ids = set(product["job_id"].astype(str)) if len(product) else set()
        first_seen = (
            df[df["job_id"].astype(str).isin(product_ids)]
            .sort_values("scd_start_date")
            .drop_duplicates(subset="job_id", keep="first")
            if product_ids
            else df.iloc[0:0].copy()
        )
        if len(first_seen):
            first_seen = first_seen.copy()
            first_seen["date"] = pd.to_datetime(first_seen["scd_start_date"]).dt.date.astype(str)
            jobs_trend = first_seen.groupby("date").size().reset_index(name="new_jobs").sort_values("date")
        else:
            jobs_trend = pd.DataFrame({"date": [], "new_jobs": []})

        # 6. Top companies (product board)
        companies_df = product.copy()
        if len(companies_df):
            companies_df["company"] = companies_df["company"].apply(lambda c: normalize_company(c))
        top_companies = (
            companies_df[companies_df["company"].notna()]
            .groupby("company")
            .size()
            .reset_index(name="job_count")
            .sort_values("job_count", ascending=False)
            .head(20)
            if len(companies_df)
            else pd.DataFrame({"company": [], "job_count": []})
        )

        # 7. Active vs expired — Active = full published board (lakehouse active)
        expired_count = int((df["is_current"] == False).sum())
        active_vs_expired = pd.DataFrame(
            [
                {"status": "Active", "job_count": len(all_jobs)},
                {"status": "Expired", "job_count": expired_count},
            ]
        )

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
        # Canonical display labels so matches render as "AWS"/"SQL"/"LLM", not "Aws"/"Sql"/"Llm"
        skill_canonical = {s.lower(): s for s in SKILL_KEYWORDS}

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

        arbeitnow_jobs = product[product["source"] == "arbeitnow"] if len(product) else product

        for _, row in product.iterrows():
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
                skill_counter[skill_canonical[match.group().lower()]] += 1

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

        english_jobs_total = (
            int(product["is_english"].astype(bool).sum()) if "is_english" in product.columns and len(product) else 0
        )

        description_insights = pd.DataFrame(
            [
                {
                    "english_jobs": english_jobs_total,
                    "arbeitnow_english_descriptions": english_count,
                    "homeoffice_mentioned": homeoffice_desc_count,
                    "jobs_with_benefits": benefits_count,
                    "arbeitnow_total": len(arbeitnow_jobs),
                }
            ]
        )

        quality_metrics = compute_quality_metrics(product if len(product) else current)
        data_quality_report = pd.DataFrame([quality_metrics])

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
        wr.s3.to_csv(data_quality_report, path=f"{gold_base}/data_quality_report.csv", index=False)

        from metrics_payload import upload_metrics_json

        upload_metrics_json(gold_bucket)
        print("Metrics snapshot written to metrics.json")

        msg = (
            f"Gold layer refreshed. Board jobs: {len(all_jobs)}, "
            f"product/audience: {len(early_career)}, "
            f"lakehouse active: {lakehouse_active}, Files written: 13"
        )
        print(msg)
        _trigger_github_redeploy(len(all_jobs))
        return {"statusCode": 200, "body": json.dumps({"message": msg})}

    except Exception as e:
        # Re-raise so the async S3-triggered invocation is retried and the DLQ/alarms fire.
        print(f"Gold generation failed: {str(e)}")
        raise


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
