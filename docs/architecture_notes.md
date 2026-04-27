# Infrastructure & Architecture Notes
## Azure Resources
| Resource | Name | Region |
|---|---|---|
| Resource Group | rg-nyctaxi-prod | East US 2 |
| Storage Account | adlsnyctaxiprod | East US 2 |
| Container | nyctaxi | — |
| ADF Instance | adf-nyctaxi-prod | East US 2 |
| Databricks Workspace | dbw-nyctaxi-prod | East US 2 |
| Databricks Cluster | cluster-nyctaxi-dev | Runtime 15.4 LTS, Spark 3.5 |
## ADLS Folder Structure
nyctaxi/
├── bronze/yellow_taxi/year=2023/month=01/ … month=12/
├── silver/yellow_taxi/          ← Delta table, partitioned by source_year/pickup_date
├── gold/trip_metrics/           ← Delta table, Delta MERGE incremental
├── gold/dim_taxi_zone/          ← Delta table, SCD Type 2
└── quarantine/yellow_taxi/      ← Parquet, bad records with rejection_reason
## Unity Catalog Tables
| Catalog | Schema | Table | Description |
|---|---|---|---|
| hive_metastore | silver | yellow_taxi | 37.8M rows, 18 columns |
| hive_metastore | gold | trip_metrics | 80,554 zone-day aggregations |
| hive_metastore | gold | dim_taxi_zone | 267 rows (265 + 2 SCD versions) |
## Key Numbers
| Metric | Value |
|---|---|
| Bronze rows | 38,310,226 |
| Silver good records | 37,840,000 |
| Quarantine records | 470,226 (1.23%) |
| Gold trip_metrics rows | 80,554 |
| Gold dim_taxi_zone rows | 267 |
| Top revenue zone | JFK Airport — $604K on 2023-11-27 |
## Airflow
- Version: 3.1.6 (Celery Executor)
- Broker: Redis
- Backend: PostgreSQL
- Schedule: Monthly — 0 0 1 * *
- DAGs: dag_bronze_ingest → dag_silver_transform → dag_gold_build
## Authentication Notes
- Current: Account key via pipeline_config (dev only)
- Production upgrade: Replace ACCOUNT_KEY with
 `dbutils.secrets.get(scope='nyctaxi-kv-scope', key='adls-account-key')`
- Best practice: Managed Identity for both ADF and Databricks → ADLS
## Known Gaps & Production Upgrade Path
| Gap | Production solution |
|---|---|
| Account key in config | Key Vault backed Databricks secret scope |
| Static 2023 processing | Dynamic year/month via Airflow parameters + Databricks widgets |
| No audit table running | audit_utils.py ready — wire into notebooks |
| Tests not running in CI | Configure Databricks Connect for cloud-based test execution |
| Airflow on local Docker | Deploy to Azure Container Instance |