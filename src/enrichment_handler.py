"""Standalone enrichment Lambda — can run after Gold or be invoked on schedule."""

import json
import os

import awswrangler as wr
import pandas as pd

from enrichment.enricher import enrich_jobs_dataframe
from ai_gateway.router import ModelRouter


def lambda_handler(event, context):
    gold_bucket = os.environ.get("GOLD_BUCKET")
    jobs_key = os.environ.get("GOLD_KEY", "all_jobs.csv")
    output_key = os.environ.get("ENRICHMENT_OUTPUT_KEY", "ai_job_enrichment.csv")
    index_key = os.environ.get("EMBEDDING_INDEX_KEY", "embedding_index.json")

    if not gold_bucket:
        return {"statusCode": 400, "body": json.dumps({"error": "GOLD_BUCKET required"})}

    try:
        jobs_path = f"s3://{gold_bucket}/{jobs_key}"
        df = wr.s3.read_csv(jobs_path)
        print(f"Loaded {len(df)} jobs for enrichment")

        enrichment_df = enrich_jobs_dataframe(df)
        gold_base = f"s3://{gold_bucket}"
        wr.s3.to_csv(enrichment_df, path=f"{gold_base}/{output_key}", index=False)
        print(f"Wrote {len(enrichment_df)} enrichment rows to {output_key}")

        from embedding_index import build_embedding_index, index_to_json

        jobs_list = df.to_dict(orient="records")
        enrich_map = enrichment_df.set_index("job_id").to_dict(orient="index") if not enrichment_df.empty else {}
        for job in jobs_list:
            jid = job.get("job_id", "")
            if jid in enrich_map:
                job.update(enrich_map[jid])

        router = ModelRouter()
        limit = int(os.environ.get("INDEX_BUILD_LIMIT", "500"))
        index = build_embedding_index(jobs_list[:limit], router)
        import boto3

        boto3.client("s3").put_object(
            Bucket=gold_bucket,
            Key=index_key,
            Body=index_to_json(index).encode("utf-8"),
            ContentType="application/json",
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "enriched": len(enrichment_df),
                "index_size": len(index),
                "cost_summary": router.cost_logger.summary(),
            }),
        }
    except Exception as e:
        print(f"Enrichment failed: {e}")
        raise
