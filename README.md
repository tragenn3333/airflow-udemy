# Currency Exchange ETL Pipeline

An Apache Airflow ETL project that fetches 30 days of live currency exchange-rate data, transforms and analyzes it, then creates a CSV file and HTML chart.

## Workflow

Extract API data → Transform rates → Analyze 30-day changes → Save CSV and HTML chart

## Run

Run `docker compose up -d`.

Open Airflow at `http://localhost:8080` and trigger `currency_exchange_etl`.