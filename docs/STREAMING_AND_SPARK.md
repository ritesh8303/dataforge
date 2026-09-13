# Streaming and Spark — design note (thin proof)

DataForge’s production path is **batch serverless** (EventBridge → Lambda → S3 Parquet → Gold CSV).
This note is the interview talk-track for “streaming / Spark” keywords without standing up a cluster.

## What we would stream

| Signal | Candidate bus | Why it is not in prod |
|--------|---------------|------------------------|
| New ATS postings within minutes | EventBridge → Lambda fan-out (already) | Daily cron meets portfolio cost (€0–25) |
| Click / match feedback | S3 → optional Kinesis Firehose → Gold | No product traffic yet |
| Embedding rebuild | Enrichment Lambda after Gold | Incremental LanceDB upsert is enough |

## Spark (talk-track + future cutover)

Silver SCD2 and Gold aggregates today use **pandas on Lambda** (AWSSDKPandas layer).
If volume outgrows ~memory/time limits:

1. Keep the **same medallion contracts** (`docs/DATA_DICTIONARY.md`).
2. Replace Silver/Gold compute with **AWS Glue Spark** or EMR Serverless reading Bronze/Silver Parquet.
3. Leave Match API + LanceDB on S3 unchanged (vector path is independent of Spark).

Local experiment (optional, not required for thesis):

```bash
# Illustrative only — not wired in CI
pyspark
# spark.read.parquet("s3a://…/bronze/…").createOrReplaceTempView("bronze")
```

## Streaming vs batch decision

- **Batch wins** while daily ingest + €40 hard cap is the constraint.
- **Streaming is justified** only if a JD requires sub-hour freshness *and* budget allows Kinesis/MSK.
- Portfolio claim: “designed for Spark cutover; currently serverless batch with Step Functions Express thin proof.”

See also: `terraform/stepfunctions.tf`, `airflow/dags/dataforge_pipeline.py`.
