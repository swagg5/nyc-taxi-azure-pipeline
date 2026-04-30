# NYC Yellow Taxi — Azure Medallion Batch Pipeline

![CI](https://github.com/swagg5/nyc-taxi-azure-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Spark](https://img.shields.io/badge/spark-3.5.0-orange)
![Airflow](https://img.shields.io/badge/airflow-3.1.6-green)
![Delta](https://img.shields.io/badge/delta-3.2.0-red)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

A production-grade batch data pipeline built on Azure, processing 38M+ NYC Yellow Taxi trip records through a Bronze → Silver → Gold medallion architecture.
---
## Architecture
NYC TLC (nyc.gov HTTP)

│

▼

┌─────────────────┐

│   Azure ADF     │  HTTP → ADLS Gen2

│  (Ingestion)    │  12 monthly Parquet files

└────────┬────────┘

│

▼

┌─────────────────┐

│     BRONZE      │  Raw Parquet, immutable

│  ADLS Gen2      │  Partitioned: year=2023/month=MM

└────────┬────────┘

│

▼

┌─────────────────┐

│     SILVER      │  Databricks + PySpark + Delta Lake

│  Delta Lake     │  Schema enforcement, DQ checks,

│                 │  Quarantine pattern

└────────┬────────┘

│

▼

┌─────────────────┐

│      GOLD       │  Delta Lake — two tables

│  Delta Lake     │  trip_metrics + dim_taxi_zone

└────────┬────────┘

│

▼

┌─────────────────┐

│    AIRFLOW      │  3 DAGs, monthly schedule

│ Orchestration   │  Bronze → Silver → Gold chain

└─────────────────┘

Official Website of New York City Government - nyc.gov
On the homepage of nyc.gov, you can check today's statuses for parking, schools, and trash collection. You can also access popular services, news, and see what's new from NYC government. 
---
## Tech Stack
| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Ingestion | Azure Data Factory (HTTP → ADLS Gen2) |
| Storage | ADLS Gen2 with hierarchical namespace |
| Compute | Azure Databricks (Spark 3.5, Runtime 15.4 LTS) |
| Table Format | Delta Lake |
| Orchestration | Apache Airflow 3.1.6 (Docker, Celery executor) |
| Governance | Unity Catalog (hive_metastore) |
| Source Data | NYC TLC Yellow Taxi Trip Records 2023 |
---
## Pipeline Layers
### Bronze
- Source: NYC TLC public HTTP endpoint (nyc.gov CDN)
- Tool: ADF HTTP connector → ADLS Gen2
- Format: Raw Parquet, immutable, never overwritten
- Partitioning: year=2023/month=MM (Hive-style)
- Volume: 12 monthly files, ~4GB total, 38.3M rows
### Silver
- Tool: Databricks PySpark notebook (02_silver_transform)
- Schema enforcement: Explicit type casting on all 19 columns
- DQ rules: 9 rules — null checks, range checks, date validity
- Quarantine pattern: Bad records routed to quarantine/yellow_taxi/ with rejection_reason column — never silently dropped
- Output: 37,840,000 good records → Delta Lake
- Quarantine rate: 1.23% (470,226 records)
- Partitioning: source_year / pickup_date
### Gold
Two tables built from Silver:
**trip_metrics** — aggregated KPIs
- Granularity: one row per pickup zone per day
- 80,554 rows across 2023
- Load pattern: Delta MERGE incremental — no full overwrites
- Business key: pickup_date + pickup_location_id
**dim_taxi_zone** — SCD Type 2 dimension
- 265 NYC TLC taxi zones
- Tracks historical changes to zone attributes
- Control columns: effective_start_date, effective_end_date, is_current
- Load pattern: Two-step Delta MERGE (close old → insert new)
---
## Orchestration
Three Airflow DAGs with monthly schedule (0 0 1 * *):

dag_bronze_ingest
 trigger_adf_pipeline → verify_bronze_files

dag_silver_transform
 wait_for_bronze → run_silver_notebook → verify_silver_quality

dag_gold_build
 wait_for_silver → [run_gold_metrics + run_gold_dim] → verify_gold_output

Cross-layer dependency: Silver only runs after Bronze succeeds. Gold only runs after Silver succeeds.
---
## Key Design Decisions
**Why ADF for ingestion only?**
ADF handles connectivity. Databricks handles computation. Mixing them makes both harder to test and maintain.

**Why Delta Lake over plain Parquet?**
ACID transactions, schema enforcement, MERGE support, and time travel. Plain Parquet has none of these.

**Why quarantine instead of dropping bad records?**
Silent drops are unauditable. The quarantine pattern routes bad records to a separate location with a rejection reason — enabling replay, investigation, and quarantine rate monitoring as a data quality KPI.

**Why SCD Type 2 for taxi zones?**
Historical trips must join to the zone attributes that were current at the time of the trip. SCD Type 1 would corrupt historical analysis by retroactively applying new zone assignments.

**Why Airflow over ADF triggers?**
Airflow DAGs are Python — version-controlled, testable, and support cross-system dependency management that ADF triggers cannot express.

---
## Key Numbers
| Metric | Value |
|---|---|
| Bronze rows | 38,310,226 |
| Silver good records | 37,840,000 |
| Quarantine records | 470,226 (1.23%) |
| Gold trip_metrics rows | 80,554 |
| Gold dim_taxi_zone rows | 267 (265 + 2 SCD versions) |
| Top revenue zone | JFK Airport — $604K on 2023-11-27 |
---
## Repository Structure
nyc-taxi-azure-pipeline/
├── airflow/
│   ├── dag_bronze_ingest.py
│   ├── dag_silver_transform.py
│   └── dag_gold_build.py
├── databricks/
├── adf/
└── README.md
---
## Data Source
NYC Taxi and Limousine Commission (TLC) Trip Record Data
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
