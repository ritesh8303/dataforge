"""Deploy src/ zip to all DataForge Lambda functions."""
from __future__ import annotations

import zipfile
from pathlib import Path

import boto3

REGION = "eu-central-1"
SRC = Path(__file__).resolve().parents[1] / "src"
ZIP_PATH = Path(__file__).resolve().parents[1] / "lambda_deploy.zip"

FUNCTIONS = [
    "dataforge-ingestor",
    "dataforge-ba-ingestor",
    "dataforge-company-ingestor",
    "dataforge-berlin-startups-ingestor",
    "dataforge-transformer",
    "dataforge-gold-generator",
    "dataforge-metrics",
    "dataforge-jobs-api",
    "dataforge-match-api",
    "dataforge-enrichment",
]

# Only the transformer must be single-flight; account concurrency limits may block more.
CONCURRENCY = {
    "dataforge-transformer": 1,
}

LAMBDA_CONFIG = {
    "dataforge-gold-generator": {"Timeout": 900, "MemorySize": 2048},
}

ENV_PATCH = {
    "dataforge-enrichment": {
        "INDEX_BUILD_LIMIT": "2500",
        "ENRICHMENT_MAX_LLM": "800",
        "AI_ENRICHMENT_SAMPLE_RATE": "1.0",
        "CLASSIFICATION_CACHE_S3": "s3://dataforge-gold-dev-366945363779/classification_cache.json",
    },
    "dataforge-match-api": {
        "INDEX_BUILD_LIMIT": "2500",
        "MATCH_RERANK": "true",
        "MATCH_BUILD_INDEX_ON_MISS": "false",
        "MATCH_RATE_LIMIT": "40",
        "ALLOWED_ORIGIN": "https://ritesh8303.github.io,http://localhost:8001,http://127.0.0.1:8001",
        "CLASSIFICATION_CACHE_S3": "s3://dataforge-gold-dev-366945363779/classification_cache.json",
    },
}


def build_zip() -> Path:
    if ZIP_PATH.exists():
        ZIP_PATH.unlink()
    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in SRC.rglob("*"):
            if path.is_file() and "__pycache__" not in path.parts:
                zf.write(path, path.relative_to(SRC))
    print(f"Built {ZIP_PATH} ({ZIP_PATH.stat().st_size // 1024} KB)")
    return ZIP_PATH


def main():
    build_zip()
    lc = boto3.client("lambda", region_name=REGION)
    with open(ZIP_PATH, "rb") as f:
        payload = f.read()
    for fn in FUNCTIONS:
        resp = lc.update_function_code(FunctionName=fn, ZipFile=payload)
        print(f"  OK {fn} -> {resp['LastModified']} ({resp['CodeSize']} bytes)")
        lc.get_waiter("function_updated").wait(FunctionName=fn, WaiterConfig={"Delay": 2, "MaxAttempts": 30})

    for fn, limit in CONCURRENCY.items():
        try:
            lc.put_function_concurrency(FunctionName=fn, ReservedConcurrentExecutions=limit)
            print(f"  OK {fn} reserved concurrency -> {limit}")
        except Exception as exc:
            print(f"  WARN {fn} concurrency not set ({exc}); S3 lock in transformer is the fallback")

    for fn, cfg in LAMBDA_CONFIG.items():
        lc.update_function_configuration(FunctionName=fn, **cfg)
        print(f"  OK {fn} config -> {cfg}")

    for fn, patch in ENV_PATCH.items():
        cur = lc.get_function_configuration(FunctionName=fn)
        env = dict((cur.get("Environment") or {}).get("Variables") or {})
        env.update(patch)
        lc.update_function_configuration(FunctionName=fn, Environment={"Variables": env})
        print(f"  OK {fn} env patch -> {sorted(patch)}")

    try:
        lc.update_function_url_config(
            FunctionName="dataforge-match-api",
            Cors={
                "AllowOrigins": [
                    "https://ritesh8303.github.io",
                    "http://localhost:8001",
                    "http://127.0.0.1:8001",
                ],
                "AllowMethods": ["GET", "POST"],
                "AllowHeaders": ["*"],
                "MaxAge": 300,
            },
        )
        print("  OK match Function URL CORS locked to GitHub Pages + localhost")
    except Exception as exc:
        print(f"  WARN Function URL CORS not updated ({exc})")

    print(f"Deployed {len(FUNCTIONS)} functions.")


if __name__ == "__main__":
    main()
