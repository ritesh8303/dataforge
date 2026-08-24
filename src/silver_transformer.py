import hashlib
import pandas as pd
import awswrangler as wr
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
import os
import re
from processing.europe_filter import is_in_europe
from processing.company_normalize import normalize_company

VALID_SOURCES = frozenset({"ba_api", "direct", "eures", "arbeitnow", "berlin_startups"})
REMOVED_SOURCES = frozenset({"indeed", "hacker_news"})
BRONZE_SOURCE_PREFIXES = (
    "arbeitnow",
    "ba_api",
    "direct_careers",
    "berlin_startups",
    "eures",
)
# Reserved for future tuning; not used for daily SCD (Bronze snapshot < cumulative Silver).
MIN_BRONZE_ACTIVE_RATIO = float(os.environ.get("MIN_BRONZE_ACTIVE_RATIO", "0.6"))
LOCK_TTL_SECONDS = int(os.environ.get("TRANSFORMER_LOCK_TTL_SECONDS", "900"))
INACTIVE_RETENTION_DAYS = int(os.environ.get("SILVER_INACTIVE_RETENTION_DAYS", "30"))
BRONZE_HISTORY_DAYS = int(os.environ.get("BRONZE_HISTORY_DAYS", "14"))
STALE_LOCK_SECONDS = int(os.environ.get("TRANSFORMER_STALE_LOCK_SECONDS", "620"))
# A job missing from its source's daily pull is only expired after this many
# consecutive calendar days, so one transient feed/board failure does not
# mass-expire jobs that are still open.
EXPIRE_GRACE_DAYS = int(os.environ.get("SILVER_EXPIRE_GRACE_DAYS", "2"))
# Completion marker (bucket-root prefix); the Gold S3 trigger is scoped to it so
# Gold never fires while Silver partitions are half-written.
GOLD_TRIGGER_PREFIX = "gold_trigger/"


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _list_silver_objects(path: str) -> list[str]:
    try:
        return wr.s3.list_objects(path=path) or []
    except wr.exceptions.NoFilesFound:
        return []
    except Exception as exc:
        print(f"Warning: Failed to list Silver objects at {path}: {exc}")
        return []


def _delete_s3_keys(bucket: str, keys: list[str]) -> int:
    if not keys:
        return 0
    s3 = boto3.client("s3")
    deleted = 0
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
        deleted += len(resp.get("Deleted", []))
    return deleted


def _inactive_prefix(silver_path: str) -> tuple[str, str]:
    bucket, key_prefix = _parse_s3_uri(silver_path)
    return bucket, f"{key_prefix}is_current=False/"


def _purge_inactive_silver(silver_path: str, max_age_days: int = INACTIVE_RETENTION_DAYS) -> int:
    """Delete inactive SCD history files older than max_age_days to limit S3 LIST/GET costs."""
    bucket, prefix = _inactive_prefix(silver_path)
    s3 = boto3.client("s3")
    cutoff = datetime.now(timezone.utc).timestamp() - (max_age_days * 86400)
    stale_keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["LastModified"].timestamp() < cutoff:
                stale_keys.append(obj["Key"])
    deleted = _delete_s3_keys(bucket, stale_keys)
    if deleted:
        print(f"Purged {deleted} inactive Silver files older than {max_age_days} days.")
    return deleted


def _clear_inactive_silver(silver_path: str) -> int:
    """Remove all inactive Silver partition files (used before force rebuild)."""
    bucket, prefix = _inactive_prefix(silver_path)
    s3 = boto3.client("s3")
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    deleted = _delete_s3_keys(bucket, keys)
    if deleted:
        print(f"Cleared {deleted} inactive Silver files before rebuild.")
    return deleted


def _read_silver_partition(objects: list[str], *, is_current: bool) -> tuple[pd.DataFrame, int]:
    dfs = []
    rows_read = 0
    for path in objects:
        if not path:
            continue
        try:
            df_part = wr.s3.read_parquet(path=path)
            if not df_part.empty:
                df_part["is_current"] = is_current
                dfs.append(df_part)
                rows_read += len(df_part)
        except Exception as exc:
            print(f"Warning: Failed to read Silver file {path}: {exc}")
    if not dfs:
        return pd.DataFrame(), 0
    return pd.concat(dfs, ignore_index=True), rows_read

def slugify(text):
    if pd.isna(text) or not text:
        return ""
    # Convert to lowercase and strip whitespace
    text = str(text).lower().strip()
    # Remove HTML / common tags
    text = re.sub(r"\(m/w/d\)|\(f/m/d\)|\(w/m/d\)|\(m/f/d\)", "", text)
    text = re.sub(r"\(senior\)|\(junior\)|\(lead\)|\(principal\)", "", text)
    # Remove common company suffixes
    text = re.sub(r"\bgmbh\b|\binc\b|\bcorp\b|\bco\b|\bltd\b|\bag\b|\bse\b", "", text)
    # Keep only alphanumeric and spaces
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    # Collapse multiple spaces into one, then join with hyphens
    words = text.split()
    return "-".join(words)


def validate_jobs(df):
    """Filter out rows that don't meet core data contracts."""
    initial_count = len(df)
    valid_mask = (
        df["title"].notna()
        & (df["title"].str.strip() != "")
        & df["company"].notna()
        & (df["company"].str.strip() != "")
        & df["url"].notna()
        & (df["url"].str.strip() != "")
    )
    df_valid = df[valid_mask].copy()
    dropped = initial_count - len(df_valid)
    if dropped > 0:
        print(f"Dropped {dropped} invalid jobs failing schema checks.")
    return df_valid


def _bronze_paths_for_date(bronze_bucket: str, date_str: str) -> list[str]:
    try:
        all_bronze_files = wr.s3.list_objects(path=f"s3://{bronze_bucket}/")
        return [
            f
            for f in all_bronze_files
            if f"ingested_at={date_str}" in f and f.endswith(".parquet")
        ]
    except Exception as e:
        print(f"Error listing Bronze bucket objects, falling back to default paths: {e}")
        return [
            f"s3://{bronze_bucket}/{prefix}/ingested_at={date_str}/jobs.parquet"
            for prefix in BRONZE_SOURCE_PREFIXES
        ]


def load_bronze_parquet_paths(paths: list[str]) -> pd.DataFrame:
    dfs = []
    for path in paths:
        try:
            if wr.s3.does_object_exist(path):
                df = wr.s3.read_parquet(path=path)
                if not df.empty:
                    if "published_at" not in df.columns:
                        df["published_at"] = ""
                    dfs.append(df)
                    print(f"Successfully loaded {len(df)} jobs from {path}")
        except Exception as e:
            print(f"Warning: Failed to read {path}: {e}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _normalize_bronze_before_dedup(bronze_df: pd.DataFrame) -> pd.DataFrame:
    if bronze_df.empty:
        return bronze_df

    if "source" in bronze_df.columns:
        before = len(bronze_df)
        bronze_df = bronze_df[~bronze_df["source"].isin(REMOVED_SOURCES)].copy()
        bronze_df = bronze_df[bronze_df["source"].isin(VALID_SOURCES)].copy()
        dropped = before - len(bronze_df)
        if dropped:
            print(f"Dropped {dropped} rows with removed or invalid source values.")

    if "company" in bronze_df.columns:
        bronze_df["company"] = bronze_df["company"].apply(
            lambda c: normalize_company(c) or ""
        )

    bronze_df = validate_jobs(bronze_df)

    if not bronze_df.empty:
        initial_len = len(bronze_df)

        def row_is_in_europe(r):
            source = r.get("source", "")
            if source in ("ba_api", "arbeitnow", "berlin_startups", "eures"):
                return True
            return is_in_europe(
                location_str=r.get("location", ""),
                title_str=r.get("title", ""),
                description_str=r.get("description", ""),
            )

        bronze_df = bronze_df[bronze_df.apply(row_is_in_europe, axis=1)].copy()
        print(f"Europe safety gate filtering: kept {len(bronze_df)} out of {initial_len} jobs.")

    return bronze_df


def _ensure_semantic_job_ids(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["semantic_key"] = df.apply(
        lambda r: f"sem_{slugify(r.get('company'))}_{slugify(r.get('title'))}_{slugify(r.get('location', ''))}",
        axis=1,
    )
    df["job_id"] = df["semantic_key"]
    return df.drop(columns=["semantic_key"], errors="ignore")


def _source_pull_index_from_normalized(normalized: pd.DataFrame) -> tuple[dict[str, set[str]], set[str]]:
    if normalized.empty or "source" not in normalized.columns:
        return {}, set()
    ids_df = _ensure_semantic_job_ids(normalized)
    sources_in_pull = set(ids_df["source"].astype(str))
    index = {
        str(source): set(group["job_id"].astype(str))
        for source, group in ids_df.groupby("source")
    }
    return index, sources_in_pull


def prepare_bronze_df(bronze_df: pd.DataFrame) -> pd.DataFrame:
    normalized = _normalize_bronze_before_dedup(bronze_df)
    if normalized.empty:
        return normalized
    return deduplicate_bronze(_ensure_semantic_job_ids(normalized))


def load_bronze_for_date(bronze_bucket: str, date_str: str) -> tuple[pd.DataFrame, dict[str, set[str]], set[str]]:
    print(f"Searching for Bronze files matching partition ingested_at={date_str}...")
    paths = _bronze_paths_for_date(bronze_bucket, date_str)
    print(f"Found {len(paths)} Bronze files for date {date_str}: {paths}")
    raw = load_bronze_parquet_paths(paths)
    normalized = _normalize_bronze_before_dedup(raw)
    source_pull_index, sources_in_pull = _source_pull_index_from_normalized(normalized)
    if normalized.empty:
        return normalized, source_pull_index, sources_in_pull
    bronze_df = deduplicate_bronze(_ensure_semantic_job_ids(normalized))
    return bronze_df, source_pull_index, sources_in_pull


def load_latest_bronze_snapshot(
    bronze_bucket: str,
) -> tuple[pd.DataFrame, dict[str, set[str]], set[str]]:
    """Load the newest Bronze partition as the active job universe (for Silver rebuild)."""
    try:
        all_bronze_files = wr.s3.list_objects(path=f"s3://{bronze_bucket}/")
    except Exception as exc:
        print(f"Error listing Bronze bucket for latest snapshot: {exc}")
        return pd.DataFrame(), {}, set()

    dates = sorted(
        {
            path.split("ingested_at=")[1].split("/")[0]
            for path in all_bronze_files
            if "ingested_at=" in path and path.endswith(".parquet")
        },
        reverse=True,
    )
    for date_str in dates:
        bronze_df, source_index, sources_in_pull = load_bronze_for_date(bronze_bucket, date_str)
        if not bronze_df.empty:
            print(f"Latest Bronze snapshot: {date_str} ({len(bronze_df)} jobs).")
            return bronze_df, source_index, sources_in_pull
    return pd.DataFrame(), {}, set()


def load_bronze_history(bronze_bucket: str, days: int = 21) -> pd.DataFrame:
    """Merge Bronze partitions from the last N days, keeping the latest row per job_id."""
    from datetime import timedelta

    today = datetime.now(timezone.utc).date()
    frames = []
    for offset in range(days):
        date_str = (today - timedelta(days=offset)).strftime("%Y-%m-%d")
        day_df, _, _ = load_bronze_for_date(bronze_bucket, date_str)
        if not day_df.empty:
            frames.append(day_df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["ingested_at"] = pd.to_datetime(combined["ingested_at"], utc=True, errors="coerce")
    combined = combined.sort_values("ingested_at", ascending=False)
    combined = combined.drop_duplicates(subset=["job_id"], keep="first")
    combined["ingested_at"] = combined["ingested_at"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    print(f"Bronze history merge: {len(combined)} unique jobs across last {days} days.")
    return combined.reset_index(drop=True)


def _validate_bronze_vs_silver(bronze_count: int, active_silver_count: int) -> None:
    """Block only clearly broken micro-batches; daily Bronze is smaller than cumulative Silver."""
    if active_silver_count > 1000 and bronze_count < 100:
        raise ValueError(
            f"DATA_QUALITY_ANOMALY: Incoming Bronze batch size ({bronze_count} jobs) "
            f"is abnormally low compared to active Silver records ({active_silver_count}). "
            f"Aborting to protect Silver database."
        )


def _validate_silver_read(
    active_rows: int,
    inactive_rows: int,
    active_files: int,
    inactive_files: int,
) -> None:
    if active_files > 0 and active_rows == 0:
        raise ValueError(
            f"SILVER_READ_ANOMALY: Active partition lists {active_files} file(s) but "
            f"0 active rows were read. Another transformer may be writing Silver. Aborting."
        )
    if inactive_files > 0 and inactive_rows == 0:
        raise ValueError(
            f"SILVER_READ_ANOMALY: Inactive partition lists {inactive_files} file(s) but "
            f"0 inactive rows were read. Aborting."
        )
    if active_rows == 0 and inactive_files > 0 and inactive_rows > 1000:
        raise ValueError(
            f"SILVER_READ_ANOMALY: Silver has {inactive_rows} historical records but "
            f"0 active records. Refusing to overwrite active Silver."
        )


def _validate_scd_result(
    current_silver_count: int,
    unchanged_count: int,
    changed_count: int,
    new_count: int,
    bronze_count: int,
) -> None:
    if (
        current_silver_count > 5000
        and unchanged_count == 0
        and changed_count == 0
        and new_count >= bronze_count * 0.95
    ):
        raise ValueError(
            f"SCD_ANOMALY: Would replace {current_silver_count} active Silver jobs with "
            f"{new_count} Bronze jobs and mark 0 unchanged. Aborting to prevent data loss."
        )


def deduplicate_bronze(df):
    """Consolidate duplicate postings within incoming bronze data using semantic matching."""
    if df.empty:
        return df

    # Create semantic key
    df["semantic_key"] = df.apply(
        lambda r: f"sem_{slugify(r.get('company'))}_{slugify(r.get('title'))}_{slugify(r.get('location', ''))}", axis=1
    )

    # Prioritize sources: direct > eures > arbeitnow > berlin_startups > ba_api
    source_priority = {"direct": 0, "eures": 1, "arbeitnow": 2, "berlin_startups": 3, "ba_api": 4}
    df["priority"] = df["source"].map(lambda s: source_priority.get(s, 9))

    # Calculate description length to keep the most detailed posting
    df["desc_len"] = df["description"].fillna("").astype(str).str.len()

    # Sort by priority, then description length
    df = df.sort_values(by=["priority", "desc_len"], ascending=[True, False])

    # Drop duplicate semantic keys, keeping the highest quality source
    df_dedup = df.drop_duplicates(subset=["semantic_key"], keep="first").copy()

    # Override job_id with the semantic key for cross-source persistence matching
    df_dedup["job_id"] = df_dedup["semantic_key"]

    # Drop temporary columns
    df_dedup.drop(columns=["semantic_key", "priority", "desc_len"], inplace=True)
    return df_dedup


def _lock_key_for_silver(silver_path: str) -> tuple[str, str]:
    without_scheme = silver_path.removeprefix("s3://")
    bucket, _, key_prefix = without_scheme.partition("/")
    base = key_prefix.split("jobs_history.parquet")[0]
    return bucket, f"{base}transformer.lock"


def _acquire_transformer_lock(silver_path: str) -> None:
    bucket, lock_key = _lock_key_for_silver(silver_path)
    s3 = boto3.client("s3")
    try:
        head = s3.head_object(Bucket=bucket, Key=lock_key)
        modified = head["LastModified"]
        if modified.tzinfo is None:
            modified = modified.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - modified).total_seconds()
        if age < LOCK_TTL_SECONDS:
            if age >= STALE_LOCK_SECONDS:
                print(
                    f"Breaking stale transformer lock ({int(age)}s old) "
                    f"from a previous crashed run."
                )
            else:
                raise ValueError(
                    f"PIPELINE_LOCK: Another transformer run holds the lock ({int(age)}s old). "
                    f"Skipping to prevent concurrent Silver writes."
                )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in {"404", "NoSuchKey", "NotFound", "403"}:
            raise
    s3.put_object(
        Bucket=bucket,
        Key=lock_key,
        Body=datetime.now(timezone.utc).isoformat().encode(),
    )


def _release_transformer_lock(silver_path: str) -> None:
    bucket, lock_key = _lock_key_for_silver(silver_path)
    boto3.client("s3").delete_object(Bucket=bucket, Key=lock_key)


def _write_gold_trigger_marker(silver_path: str, now: datetime) -> None:
    """Write the completion marker that triggers the Gold generator.

    The S3 notification is scoped to GOLD_TRIGGER_PREFIX, so Gold only runs
    once per transform, and only after both Silver partitions are fully written.
    """
    bucket, _ = _parse_s3_uri(silver_path)
    marker = pd.DataFrame([{"run_completed_at": now.isoformat()}])
    wr.s3.to_parquet(df=marker, path=f"s3://{bucket}/{GOLD_TRIGGER_PREFIX}run_complete.parquet")
    print("Gold trigger marker written.")


def lambda_handler(event, context):
    silver_path = os.environ.get("SILVER_PATH")
    gold_bucket = os.environ.get("GOLD_BUCKET")
    bronze_bucket = os.environ.get("BRONZE_BUCKET")

    if not silver_path:
        raise ValueError("SILVER_PATH environment variable is not set.")
    if not bronze_bucket:
        raise ValueError("BRONZE_BUCKET environment variable is not set.")

    event = event if isinstance(event, dict) else {}
    force_rebuild = bool(event.get("force_rebuild"))

    lock_held = False
    try:
        _acquire_transformer_lock(silver_path)
        lock_held = True

        if force_rebuild:
            print("Force rebuild requested — loading latest Bronze snapshot.")
            bronze_df, source_pull_index, sources_in_pull = load_latest_bronze_snapshot(bronze_bucket)
            if bronze_df.empty:
                bronze_df, source_pull_index, sources_in_pull = load_bronze_for_date(
                    bronze_bucket, datetime.now(timezone.utc).strftime("%Y-%m-%d")
                )
            if bronze_df.empty:
                print("Force rebuild: no Bronze data found. Exiting.")
                return {"statusCode": 200, "body": "No Bronze data available for rebuild."}
            process_scd_type_2(
                bronze_df,
                silver_path,
                gold_bucket,
                force_rebuild=True,
                source_pull_index=source_pull_index,
                sources_in_pull=sources_in_pull,
            )
            return {"statusCode": 200, "body": "Silver force rebuild completed."}

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if event.get("date"):
            today_str = event["date"]
            print(f"Manual date override detected: running for date {today_str}")

        active_objects = _list_silver_objects(f"{silver_path}is_current=True/")
        inactive_objects = _list_silver_objects(f"{silver_path}is_current=False/")
        if not active_objects and inactive_objects:
            print(
                f"Auto-healing Silver: 0 active files, {len(inactive_objects)} inactive — "
                "rebuilding from latest Bronze snapshot."
            )
            bronze_df, source_pull_index, sources_in_pull = load_latest_bronze_snapshot(bronze_bucket)
            if bronze_df.empty:
                bronze_df, source_pull_index, sources_in_pull = load_bronze_for_date(
                    bronze_bucket, today_str
                )
            if bronze_df.empty:
                print(f"No Bronze data available for auto-heal on {today_str}. Exiting.")
                return {"statusCode": 200, "body": f"No Bronze data for auto-heal on {today_str}."}
            process_scd_type_2(
                bronze_df,
                silver_path,
                gold_bucket,
                force_rebuild=True,
                source_pull_index=source_pull_index,
                sources_in_pull=sources_in_pull,
            )
            return {"statusCode": 200, "body": "Silver auto-heal rebuild completed."}

        bronze_df, source_pull_index, sources_in_pull = load_bronze_for_date(bronze_bucket, today_str)
        if bronze_df.empty:
            print(f"No new Bronze files found for date {today_str}. Exiting.")
            return {"statusCode": 200, "body": f"No Bronze files found for date {today_str}."}

        process_scd_type_2(
            bronze_df,
            silver_path,
            gold_bucket,
            source_pull_index=source_pull_index,
            sources_in_pull=sources_in_pull,
        )
        return {"statusCode": 200, "body": "Silver transformer execution completed."}
    finally:
        if lock_held:
            try:
                _release_transformer_lock(silver_path)
            except Exception as exc:
                print(f"Warning: failed to release transformer lock: {exc}")


def generate_hash(df, cols):
    """Creates a SHA256 hash of specific columns to detect attribute changes."""
    # Only hash columns that actually exist in the dataframe
    existing_cols = [c for c in cols if c in df.columns]
    return df[existing_cols].apply(
        lambda x: hashlib.sha256("".join(str(val) for val in x).encode()).hexdigest(), axis=1
    )


def _expire_jobs_missing_from_source_pull(
    current_silver: pd.DataFrame,
    source_pull_index: dict[str, set[str]],
    sources_in_pull: set[str],
    now: datetime,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Expire active jobs whose source was ingested today but no longer lists them.

    Grace period: a job is only expired after it has been missing from its
    source for EXPIRE_GRACE_DAYS consecutive calendar days (tracked via the
    last_seen_at column), so a single transient board/feed failure does not
    mass-expire jobs that are still open.
    """
    if current_silver.empty or not sources_in_pull:
        return current_silver, pd.DataFrame()

    df = current_silver.copy()
    sources = df["source"].astype(str) if "source" in df.columns else pd.Series("", index=df.index)
    job_ids = df["job_id"].astype(str)

    in_pull = sources.isin(sources_in_pull)
    listed = pd.Series(
        [jid in source_pull_index.get(src, set()) for src, jid in zip(sources, job_ids)],
        index=df.index,
    )

    now_ts = pd.Timestamp(now)
    if "last_seen_at" in df.columns:
        last_seen = pd.to_datetime(df["last_seen_at"], errors="coerce", utc=True)
    else:
        last_seen = pd.to_datetime(pd.Series(pd.NaT, index=df.index), utc=True)
    # Rows without last_seen_at (pre-migration) count as just-seen: they become
    # eligible for expiry only after a full grace window has elapsed.
    last_seen = last_seen.fillna(now_ts)

    days_missing = (now_ts.normalize() - last_seen.dt.normalize()).dt.days
    missing = in_pull & ~listed
    expire_mask = missing & (days_missing >= EXPIRE_GRACE_DAYS)

    # Jobs listed today get a fresh last_seen_at; grace-pending rows keep theirs.
    df["last_seen_at"] = last_seen.apply(lambda t: t.isoformat())
    df.loc[in_pull & listed, "last_seen_at"] = now_ts.isoformat()

    grace_pending = int((missing & ~expire_mask).sum())
    if grace_pending:
        print(
            f"Source-pull grace: {grace_pending} jobs missing from today's pull, "
            f"kept within the {EXPIRE_GRACE_DAYS}-day grace window."
        )

    source_expired = df[expire_mask].copy()
    if not source_expired.empty:
        source_expired["is_current"] = False
        source_expired["scd_end_date"] = now
    still_active = df[~expire_mask].copy()
    return still_active, source_expired


def process_scd_type_2(
    bronze_df,
    silver_path,
    gold_bucket=None,
    *,
    force_rebuild: bool = False,
    source_pull_index: dict[str, set[str]] | None = None,
    sources_in_pull: set[str] | None = None,
):
    """
    Implements SCD Type 2 logic:
    1. Identifies new records.
    2. Identifies changed records (expires old, inserts new version).
    3. Persists results to the Silver Layer.
    """
    now = datetime.now(timezone.utc)

    # Columns used to detect changes. source/url are intentionally excluded:
    # cross-source dedup can flip the winning source for the same semantic job
    # (e.g. when one board fails transiently), and that must not count as an
    # update or it churns the SCD history.
    attr_cols = ["title", "company", "location", "job_types"]

    # Validate that job_id exists — both ingestors now produce this column
    if "job_id" not in bronze_df.columns:
        raise ValueError(
            "Bronze data is missing 'job_id' column. Ensure ingestors rename slug/refnr to job_id before writing."
        )

    # Ensure bronze_df job_ids are migrated to semantic IDs if they aren't already
    is_old_bronze = ~bronze_df["job_id"].astype(str).str.startswith("sem_")
    if is_old_bronze.any():
        bronze_df.loc[is_old_bronze, "job_id"] = bronze_df.loc[is_old_bronze].apply(
            lambda r: f"sem_{slugify(r.get('company'))}_{slugify(r.get('title'))}_{slugify(r.get('location', ''))}",
            axis=1,
        )

    # 1. Prepare incoming Bronze data
    bronze_df = bronze_df.copy()
    bronze_df["hash_key"] = generate_hash(bronze_df, attr_cols)
    bronze_df["scd_start_date"] = now
    bronze_df["scd_end_date"] = pd.to_datetime(pd.Series(pd.NaT, index=bronze_df.index), utc=True)
    bronze_df["is_current"] = True
    bronze_df["last_seen_at"] = now.isoformat()

    if force_rebuild:
        _clear_inactive_silver(silver_path)
    else:
        _purge_inactive_silver(silver_path)

    # 2. Load existing Silver data (active partition only — inactive is append-only history)
    silver_exists = False
    silver_df = pd.DataFrame()
    active_objects: list = []
    inactive_objects: list = []

    try:
        active_path = f"{silver_path}is_current=True/"
        inactive_path = f"{silver_path}is_current=False/"

        active_objects = _list_silver_objects(active_path)
        inactive_objects = [] if force_rebuild else _list_silver_objects(inactive_path)

        silver_df, active_rows_read = _read_silver_partition(active_objects, is_current=True)
        inactive_rows_read = 0
        if not force_rebuild and inactive_objects:
            _, inactive_rows_read = _read_silver_partition(inactive_objects, is_current=False)

        if not force_rebuild:
            _validate_silver_read(
                active_rows_read,
                inactive_rows_read,
                len(active_objects),
                len(inactive_objects),
            )

        if not silver_df.empty:
            silver_df["is_current"] = silver_df["is_current"].astype(bool)
            silver_exists = True
            print(f"Loaded {len(silver_df)} active Silver records.")
            if not silver_df.empty:
                for date_col in ["scd_start_date", "scd_end_date"]:
                    if date_col in silver_df.columns:
                        silver_df[date_col] = pd.to_datetime(silver_df[date_col], errors="coerce")
                        if silver_df[date_col].dt.tz is None:
                            silver_df[date_col] = silver_df[date_col].dt.tz_localize("UTC")
                        else:
                            silver_df[date_col] = silver_df[date_col].dt.tz_convert("UTC")
            if not silver_df.empty and "job_id" in silver_df.columns:
                is_old = ~silver_df["job_id"].astype(str).str.startswith("sem_")
                if is_old.any():
                    print(f"Migrating {is_old.sum()} old Silver job IDs to semantic IDs...")
                    silver_df.loc[is_old, "job_id"] = silver_df.loc[is_old].apply(
                        lambda r: (
                            f"sem_{slugify(r.get('company'))}_{slugify(r.get('title'))}_{slugify(r.get('location', ''))}"
                        ),
                        axis=1,
                    )
            if not silver_df.empty:
                # Always recompute against the current attr_cols so a change to
                # the hash formula never masquerades as a mass job update.
                silver_df["hash_key"] = generate_hash(silver_df, attr_cols)
        else:
            print("Silver layer is empty. Performing initial load.")

    except ValueError:
        raise
    except Exception as e:
        # Re-raise any real error so we don't silently overwrite Silver with bad data
        raise RuntimeError(f"Failed to read Silver layer: {str(e)}") from e

    # Data Quality Gate Check
    active_silver_count = len(silver_df[silver_df["is_current"] == True]) if not silver_df.empty else 0
    if not force_rebuild:
        _validate_bronze_vs_silver(len(bronze_df), active_silver_count)

    # First run — write everything directly
    if not silver_exists or silver_df.empty or force_rebuild:
        if not force_rebuild and (len(active_objects) > 0 or len(inactive_objects) > 0):
            raise ValueError(
                "SILVER_READ_ANOMALY: Silver partition files exist on S3 but no rows "
                "were loaded. Refusing initial-load overwrite."
            )
        for col in bronze_df.select_dtypes(include="object").columns:
            bronze_df[col] = bronze_df[col].astype(str)
        wr.s3.to_parquet(
            df=bronze_df.drop(columns=["is_current"], errors="ignore"),
            path=f"{silver_path}is_current=True/",
            dataset=True,
            mode="overwrite",
        )
        print(f"Initial load complete. Wrote {len(bronze_df)} records to Silver.")
        if gold_bucket:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            stats = pd.DataFrame(
                [
                    {
                        "date": today,
                        "new_jobs": len(bronze_df),
                        "updated_jobs": 0,
                        "unchanged_jobs": 0,
                        "total_silver": len(bronze_df),
                        "run_at": datetime.now(timezone.utc).isoformat(),
                    }
                ]
            )
            wr.s3.to_csv(stats, path=f"s3://{gold_bucket}/pipeline_stats.csv", index=False)
            print(f"Pipeline stats written to Gold: new={len(bronze_df)}, updated=0")
        _write_gold_trigger_marker(silver_path, now)
        return

    # 3. Separate current from historical Silver records (inactive history not loaded on normal runs)
    current_silver = silver_df[silver_df["is_current"]].copy()
    historical_records = pd.DataFrame()
    current_silver_count = len(current_silver)

    if source_pull_index is None or sources_in_pull is None:
        if "source" in bronze_df.columns and not bronze_df.empty:
            source_pull_index = {
                str(source): set(group["job_id"].astype(str))
                for source, group in bronze_df.groupby("source")
            }
            sources_in_pull = set(source_pull_index.keys())
        else:
            source_pull_index, sources_in_pull = {}, set()

    current_silver, source_expired = _expire_jobs_missing_from_source_pull(
        current_silver, source_pull_index, sources_in_pull, now
    )
    source_expired_ids = set(source_expired["job_id"].astype(str)) if not source_expired.empty else set()
    if source_expired_ids:
        print(
            f"Source-pull expiration: {len(source_expired_ids)} jobs removed from active Silver "
            f"(no longer returned by their source in today's ingest)."
        )

    # 4. Detect changes by merging on job_id
    merged = pd.merge(
        current_silver[["job_id", "hash_key"]],
        bronze_df[["job_id", "hash_key"]],
        on="job_id",
        how="outer",
        suffixes=("_old", "_new"),
    )

    # Case A — job exists in Silver but hash changed → expire old record
    changed_mask = (
        merged["hash_key_old"].notna()
        & merged["hash_key_new"].notna()
        & (merged["hash_key_old"] != merged["hash_key_new"])
    )
    changed_ids = merged.loc[changed_mask, "job_id"].tolist()

    expired_records = current_silver[current_silver["job_id"].isin(changed_ids)].copy()
    expired_records["is_current"] = False
    expired_records["scd_end_date"] = now
    if not source_expired.empty:
        expired_records = pd.concat([expired_records, source_expired], ignore_index=True)

    # Case B — new job_id not in Silver at all → insert
    new_mask = merged["hash_key_old"].isna()
    new_ids = merged.loc[new_mask, "job_id"].tolist()

    # New inserts = brand new jobs + updated versions of changed jobs
    insert_ids = set(new_ids + changed_ids)
    new_inserts = bronze_df[bronze_df["job_id"].isin(insert_ids)].copy()

    # 5. Unchanged current records — keep as-is (excluding hash-updated and source-expired)
    unchanged_silver = current_silver[~current_silver["job_id"].isin(changed_ids)].copy()

    _validate_scd_result(
        current_silver_count,
        len(unchanged_silver),
        len(changed_ids),
        len(new_ids),
        len(bronze_df),
    )

    # 6. Write back to Silver S3 (partitioned, incremental)
    # Write active records (overwriting active partition)
    active_df = pd.concat([unchanged_silver, new_inserts], ignore_index=True)
    active_df = active_df.sort_values(by=["job_id", "scd_start_date"], ascending=[True, False])
    active_df = active_df.drop_duplicates(subset=["job_id"], keep="first")
    for col in active_df.select_dtypes(include="object").columns:
        active_df[col] = active_df[col].astype(str)

    wr.s3.to_parquet(
        df=active_df.drop(columns=["is_current"], errors="ignore"),
        path=f"{silver_path}is_current=True/",
        dataset=True,
        mode="overwrite",
    )

    # Append newly expired records to False partition
    if not expired_records.empty:
        for col in expired_records.select_dtypes(include="object").columns:
            expired_records[col] = expired_records[col].astype(str)
        wr.s3.to_parquet(
            df=expired_records.drop(columns=["is_current"], errors="ignore"),
            path=f"{silver_path}is_current=False/",
            dataset=True,
            mode="append",
        )

    # 7. Locally construct final_df representation for stats
    final_df = pd.concat([historical_records, unchanged_silver, expired_records, new_inserts], ignore_index=True)
    final_df = final_df.sort_values(by=["job_id", "scd_start_date"], ascending=[True, False])
    final_df = final_df.drop_duplicates(subset=["job_id", "is_current", "scd_start_date"], keep="first")

    print(
        f"SCD Type 2 complete. "
        f"New: {len(new_ids)}, Updated: {len(changed_ids)}, "
        f"Expired (missing from source): {len(source_expired_ids)}, "
        f"Unchanged: {len(unchanged_silver)}, "
        f"Total Silver records (reconstructed): {len(final_df)}"
    )

    # Write pipeline stats to Gold so dashboard can show real-time SCD metrics
    if gold_bucket:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        stats = pd.DataFrame(
            [
                {
                    "date": today,
                    "new_jobs": len(new_ids),
                    "updated_jobs": len(changed_ids),
                    "expired_jobs": len(source_expired_ids),
                    "unchanged_jobs": len(unchanged_silver),
                    "total_silver": len(final_df),
                    "run_at": datetime.now(timezone.utc).isoformat(),
                }
            ]
        )
        wr.s3.to_csv(stats, path=f"s3://{gold_bucket}/pipeline_stats.csv", index=False)
        print(
            f"Pipeline stats written to Gold: new={len(new_ids)}, "
            f"updated={len(changed_ids)}, expired={len(source_expired_ids)}"
        )

    _write_gold_trigger_marker(silver_path, now)
