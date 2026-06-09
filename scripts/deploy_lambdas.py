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
    print(f"Deployed {len(FUNCTIONS)} functions.")


if __name__ == "__main__":
    main()
