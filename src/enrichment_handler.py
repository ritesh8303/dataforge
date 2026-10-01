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

        # Prefer merging with prior enrichment so provider outages do not wipe labels.
        prior_enrich: pd.DataFrame | None = None
        try:
            prior_enrich = wr.s3.read_csv(f"s3://{gold_bucket}/{output_key}")
        except Exception:
            prior_enrich = None

        enrichment_df = enrich_jobs_dataframe(df)
        if prior_enrich is not None and not prior_enrich.empty and "job_id" in prior_enrich.columns:
            # Keep prior rows for job_ids not in this sample.
            new_ids = set(enrichment_df["job_id"].astype(str)) if not enrichment_df.empty else set()
            keep = prior_enrich[~prior_enrich["job_id"].astype(str).isin(new_ids)]
            enrichment_df = pd.concat([keep, enrichment_df], ignore_index=True)

        gold_base = f"s3://{gold_bucket}"
        wr.s3.to_csv(enrichment_df, path=f"{gold_base}/{output_key}", index=False)
        print(f"Wrote {len(enrichment_df)} enrichment rows to {output_key}")

        from embedding_index import build_embedding_index, index_from_json, index_to_json

        jobs_list = df.to_dict(orient="records")
        enrich_map = enrichment_df.set_index("job_id").to_dict(orient="index") if not enrichment_df.empty else {}
        for job in jobs_list:
            jid = job.get("job_id", "")
            if jid in enrich_map:
                job.update(enrich_map[jid])

        # Embed product-board jobs first so Match coverage hits the seeker surface.
        def _embed_priority(job: dict) -> int:
            score = 0
            if str(job.get("audience_accept") or "").lower() in {"1", "true", "yes"}:
                score += 20
            if str(job.get("ai_entry_level") or "").lower() in {"1", "true", "yes"}:
                score += 5
            if str(job.get("ai_field") or job.get("field") or ""):
                score += 2
            return score

        jobs_list.sort(key=_embed_priority, reverse=True)

        # Incremental embeddings: reuse unchanged content_hash entries.
        existing_index: list[dict] = []
        try:
            import boto3

            raw = boto3.client("s3").get_object(Bucket=gold_bucket, Key=index_key)["Body"].read()
            existing_index = index_from_json(raw)
            print(f"Loaded existing embedding index size={len(existing_index)}")
        except Exception as exc:
            print(f"No prior embedding index ({exc})")

        router = ModelRouter()
        limit = int(os.environ.get("INDEX_BUILD_LIMIT", "500"))
        index = build_embedding_index(
            jobs_list[:limit],
            router,
            existing=existing_index,
            force=False,
        )
        import boto3

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=gold_bucket,
            Key=index_key,
            Body=index_to_json(index).encode("utf-8"),
            ContentType="application/json",
        )

        reused = sum(
            1
            for e in index
            if any(
                p.get("job_id") == e.get("job_id") and p.get("content_hash") == e.get("content_hash")
                for p in existing_index
            )
        )
        print(f"Embedding index size={len(index)} reused≈{reused}")

        vector_uri = os.environ.get("VECTOR_STORE_URI", "").strip()
        vector_backend = "json"
        if vector_uri:
            from vector_store import build_store_from_embedding_index

            store = build_store_from_embedding_index(index, uri=vector_uri)
            vector_backend = store.backend
            print(f"Vector store backend={vector_backend} size={len(store.vectors_by_id())} uri={vector_uri}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "enriched": len(enrichment_df),
                "index_size": len(index),
                "index_reused_approx": reused,
                "vector_backend": vector_backend,
                "cost_summary": router.cost_logger.summary(),
            }),
        }
    except Exception as e:
        print(f"Enrichment failed: {e}")
        raise
