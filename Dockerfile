FROM python:3.11-slim-bookworm

WORKDIR /app

COPY requirements-test.txt requirements-analytics.txt ./
RUN pip install --no-cache-dir -r requirements-test.txt -r requirements-analytics.txt

COPY . .

# Gold analytics path only (quality gate + dbt). Not a substitute for the AWS lakehouse.
CMD ["sh", "-c", "python scripts/check_quality_gate.py && dbt run --project-dir dbt --profiles-dir dbt && dbt test --project-dir dbt --profiles-dir dbt"]
