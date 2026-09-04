.PHONY: quality test dbt analytics docker

quality:
	python scripts/check_quality_gate.py

test:
	pytest tests/ --ignore=tests/test_e2e_pipeline.py -v --tb=short

dbt:
	dbt run --project-dir dbt --profiles-dir dbt
	dbt test --project-dir dbt --profiles-dir dbt

analytics: quality dbt

docker:
	docker compose run --rm analytics
