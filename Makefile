.PHONY: install test lint dbt-local dag-check

install:
	pip install -r requirements.txt

test:
	pytest -q

lint:
	ruff check .

# Run the whole dbt project offline on DuckDB using the seeded sample.
dbt-local:
	cd dbt && dbt seed --target duckdb && dbt build --target duckdb

dag-check:
	python -c "from airflow.models import DagBag; \
	           d=DagBag('dags'); assert not d.import_errors, d.import_errors; \
	           print('DAG import OK')"