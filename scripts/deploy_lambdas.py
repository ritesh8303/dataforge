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
]

# Only the transformer must be single-flight; account concurrency limits may block more.
CONCURRENCY = {
    "dataforge-transformer": 1,
}

LAMBDA_CONFIG = {
    "dataforge-gold-generator": {"Timeout": 900, "MemorySize": 2048},
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

    for fn, limit in CONCURRENCY.items():
        try:
            lc.put_function_concurrency(FunctionName=fn, ReservedConcurrentExecutions=limit)
            print(f"  OK {fn} reserved concurrency -> {limit}")
        except Exception as exc:
            print(f"  WARN {fn} concurrency not set ({exc}); S3 lock in transformer is the fallback")

    for fn, cfg in LAMBDA_CONFIG.items():
        lc.update_function_configuration(FunctionName=fn, **cfg)
        print(f"  OK {fn} config -> {cfg}")

    print(f"Deployed {len(FUNCTIONS)} functions.")


if __name__ == "__main__":
    main()
