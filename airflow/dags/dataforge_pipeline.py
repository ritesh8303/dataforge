"""DataForge Bronze→Silver→Gold→enrich DAG — local Docker / Airflow only.

AWS production stays on EventBridge + Lambda (+ optional Step Functions Express).
This DAG is the interview keyword proof and local reproducibility path.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

REPO = Path("/opt/airflow/dataforge")
# Fallback when mounting repo at /app (compose analytics style)
if not (REPO / "src").exists():
    REPO = Path("/app")


def _mark(step: str) -> None:
    print(f"[dataforge] {step} @ {datetime.utcnow().isoformat()}Z")


default_args = {
    "owner": "dataforge",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dataforge_pipeline",
    description="Local medallion proof: Bronze ingest → Silver SCD2 → Gold → enrich (optional)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual / demo only — avoid accidental cloud spend from local triggers
    catchup=False,
    tags=["dataforge", "medallion", "portfolio"],
) as dag:
    start = PythonOperator(
        task_id="plan",
        python_callable=_mark,
        op_args=["plan Bronze→Silver→Gold→enrich"],
    )

    # Local scripts mirror Lambda handlers when AWS creds + buckets are set.
    bronze = BashOperator(
        task_id="bronze_ingest_arbeitnow",
        bash_command=f"cd {REPO} && PYTHONPATH=src:. python scripts/run_ingestor_local.py arbeitnow || echo 'skip: no creds/network'",
    )

    silver = BashOperator(
        task_id="silver_scd2",
        bash_command=f"cd {REPO} && PYTHONPATH=src:. python -c \"from silver_transformer import lambda_handler; print(lambda_handler({{}}, None))\" || echo 'skip: silver needs S3'",
    )

    gold = BashOperator(
        task_id="gold_marts",
        bash_command=f"cd {REPO} && PYTHONPATH=src:. python -c \"from gold_generator import lambda_handler; print(lambda_handler({{}}, None))\" || echo 'skip: gold needs S3'",
    )

    enrich = BashOperator(
        task_id="enrich_rules_first",
        bash_command=(
            f"cd {REPO} && PYTHONPATH=src:. "
            "python -c \"from enrichment_handler import lambda_handler; "
            "import os; os.environ.setdefault('AI_ENABLED','false'); "
            "print(lambda_handler({}, None))\" || echo 'skip: enrichment optional'"
        ),
    )

    quality = BashOperator(
        task_id="quality_gate",
        bash_command=f"cd {REPO} && python scripts/check_quality_gate.py",
    )

    start >> bronze >> silver >> gold >> enrich >> quality
