# dag_bronze_ingest.py
# ─────────────────────────────────────────────────────────────────
# DAG 1 — Bronze Ingestion
# Responsibility: Trigger the ADF pipeline that downloads 12 monthly
# Parquet files from nyc.gov and lands them in ADLS Gen2 Bronze layer.
#
# In this portfolio version we simulate the ADF trigger using a
# PythonOperator. In production this would use the
# AzureDataFactoryRunPipelineOperator from apache-airflow-providers-microsoft-azure
# ─────────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# ── DEFAULT ARGS ───────────────────────────────────────────────────
# These apply to every task in the DAG unless overridden at task level
# retries=2 means if a task fails, Airflow tries 2 more times
# retry_delay=5 minutes between retries
default_args = {
   "owner": "nyctaxi_pipeline",
   "depends_on_past": False,       # Each run is independent
   "email_on_failure": False,      # Set to True + add email in production
   "retries": 2,
   "retry_delay": timedelta(minutes=5),
}
# ── DAG DEFINITION ─────────────────────────────────────────────────
# schedule="@monthly" runs on the 1st of every month at midnight
# catchup=False means don't backfill missed runs
dag = DAG(
   dag_id="dag_bronze_ingest",
   default_args=default_args,
   description="Trigger ADF pipeline to land Bronze Parquet files",
   schedule="@monthly",
   start_date=datetime(2023, 1, 1),
   catchup=False,
   tags=["nyctaxi", "bronze", "ingestion"],
)
# ── TASK FUNCTIONS ─────────────────────────────────────────────────
def trigger_adf_pipeline(**context):
   """
   In production: calls AzureDataFactoryRunPipelineOperator
   which triggers pl_bronze_ingest_yellow_taxi in ADF.
   For portfolio: simulates the trigger and logs what would happen.
   The ADF pipeline already ran manually in Week 1 and all 12 files
   are confirmed in Bronze — this DAG documents the orchestration pattern.
   """
   print("=" * 60)
   print("TASK: trigger_adf_pipeline")
   print("ADF Factory  : adf-nyctaxi-prod")
   print("Resource Group: rg-nyctaxi-prod")
   print("Pipeline     : pl_bronze_ingest_yellow_taxi")
   print("Action       : Triggering ForEach over 12 months")
   print("Expected     : 12 Parquet files → Bronze ADLS layer")
   print("=" * 60)
   # In production:
   # from airflow.providers.microsoft.azure.operators.data_factory import (
   #     AzureDataFactoryRunPipelineOperator
   # )
   # AzureDataFactoryRunPipelineOperator(
   #     task_id="trigger_adf",
   #     azure_data_factory_conn_id="azure_data_factory",
   #     factory_name="adf-nyctaxi-prod",
   #     resource_group_name="rg-nyctaxi-prod",
   #     pipeline_name="pl_bronze_ingest_yellow_taxi",
   # )
   return "ADF pipeline triggered successfully"

def verify_bronze_files(**context):
   """
   Verifies that all 12 monthly Parquet files landed in Bronze.
   In production: uses azure-storage-blob SDK to list ADLS paths.
   Fails the task (raises exception) if file count != 12.
   """
   print("=" * 60)
   print("TASK: verify_bronze_files")
   print("Checking Bronze path:")
   print("nyctaxi/bronze/yellow_taxi/year=2023/")
   print("Expected: 12 month= partitions")
   # Simulate verification
   expected_months = [f"{m:02d}" for m in range(1, 13)]
   verified_months = expected_months  # In production: list from ADLS
   if len(verified_months) != 12:
       raise ValueError(
           f"Bronze file count mismatch. "
           f"Expected 12, found {len(verified_months)}"
       )
   print(f"Verified months: {verified_months}")
   print("Bronze verification PASSED — 12/12 files confirmed")
   print("=" * 60)
   return "Bronze verification passed"

# ── TASK DEFINITIONS ───────────────────────────────────────────────
# Each PythonOperator wraps one function as one Airflow task
task_trigger_adf = PythonOperator(
   task_id="trigger_adf_pipeline",
   python_callable=trigger_adf_pipeline,
   dag=dag,
)
task_verify_bronze = PythonOperator(
   task_id="verify_bronze_files",
   python_callable=verify_bronze_files,
   dag=dag,
)
# ── TASK DEPENDENCIES ──────────────────────────────────────────────
# >> means "then" — trigger first, verify after
# If trigger fails, verify never runs
task_trigger_adf >> task_verify_bronze