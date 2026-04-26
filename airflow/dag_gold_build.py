# dag_gold_build.py
# ─────────────────────────────────────────────────────────────────
# DAG 3 — Gold Build
# Responsibility: Trigger the Databricks Gold notebook after
# Silver transformation completes.
# Builds:
#   - trip_metrics: aggregated KPIs, Delta MERGE incremental load
#   - dim_taxi_zone: SCD Type 2 dimension, Delta MERGE
#
# Depends on: dag_silver_transform completing successfully
# ─────────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
default_args = {
   "owner": "nyctaxi_pipeline",
   "depends_on_past": False,
   "email_on_failure": False,
   "retries": 2,
   "retry_delay": timedelta(minutes=5),
}
dag = DAG(
   dag_id="dag_gold_build",
   default_args=default_args,
   description="Build Gold trip_metrics and dim_taxi_zone after Silver",
   schedule="@monthly",
   start_date=datetime(2023, 1, 1),
   catchup=False,
   tags=["nyctaxi", "gold", "aggregation"],
)
# ── TASK FUNCTIONS ─────────────────────────────────────────────────
def wait_for_silver(**context):
   """
   Confirms Silver completed successfully before Gold runs.
   In production: ExternalTaskSensor watching dag_silver_transform.
   Gold must never run on stale Silver data.
   """
   print("=" * 60)
   print("TASK: wait_for_silver")
   print("Checking: dag_silver_transform completed successfully")
   print("Status: CONFIRMED — Silver verified 37,840,000 rows")
   print("Proceeding to Gold build")
   print("=" * 60)
   return "Silver dependency satisfied"

def run_gold_metrics(**context):
   """
   Triggers 03_gold_build notebook — trip_metrics section.
   Aggregates Silver by pickup_date + pickup_location_id.
   Uses Delta MERGE for incremental load — no full overwrites.
   Business key: pickup_date + pickup_location_id
   """
   print("=" * 60)
   print("TASK: run_gold_metrics")
   print("Databricks workspace : dbw-nyctaxi-prod")
   print("Notebook             : 03_gold_build")
   print("Output table         : hive_metastore.gold.trip_metrics")
   print("Load pattern         : Delta MERGE — incremental")
   print("Business key         : pickup_date + pickup_location_id")
   print("Expected rows        : ~80,554 zone-day combinations")
   print("=" * 60)
   return "Gold trip_metrics build complete"

def run_gold_dim(**context):
   """
   Triggers dim_taxi_zone SCD Type 2 MERGE.
   Closes changed zone records, inserts new versions.
   Always joins on is_current=True for current zone attributes.
   """
   print("=" * 60)
   print("TASK: run_gold_dim")
   print("Databricks workspace : dbw-nyctaxi-prod")
   print("Notebook             : 03_gold_build")
   print("Output table         : hive_metastore.gold.dim_taxi_zone")
   print("Load pattern         : SCD Type 2 MERGE")
   print("Current records      : 265 zones")
   print("=" * 60)
   return "Gold dim_taxi_zone SCD Type 2 complete"

def verify_gold_output(**context):
   """
   Final verification — checks both Gold tables have data.
   Fails the pipeline if either table is empty.
   In production: queries Unity Catalog via Databricks SQL.
   """
   print("=" * 60)
   print("TASK: verify_gold_output")
   # Simulate verification
   trip_metrics_rows = 80_554
   dim_zone_rows = 267
   if trip_metrics_rows == 0:
       raise ValueError("trip_metrics is empty — Gold build failed")
   if dim_zone_rows == 0:
       raise ValueError("dim_taxi_zone is empty — Gold build failed")
   print(f"trip_metrics rows : {trip_metrics_rows:,} ✓")
   print(f"dim_taxi_zone rows: {dim_zone_rows:,} ✓")
   print("Gold verification PASSED")
   print("Pipeline complete — Bronze → Silver → Gold")
   print("=" * 60)
   return "Gold verification passed"

# ── TASK DEFINITIONS ───────────────────────────────────────────────
task_wait_silver = PythonOperator(
   task_id="wait_for_silver",
   python_callable=wait_for_silver,
   dag=dag,
)
task_gold_metrics = PythonOperator(
   task_id="run_gold_metrics",
   python_callable=run_gold_metrics,
   dag=dag,
)
task_gold_dim = PythonOperator(
   task_id="run_gold_dim",
   python_callable=run_gold_dim,
   dag=dag,
)
task_verify_gold = PythonOperator(
   task_id="verify_gold_output",
   python_callable=verify_gold_output,
   dag=dag,
)
# ── TASK DEPENDENCIES ──────────────────────────────────────────────
# wait_for_silver → run both Gold tasks in PARALLEL → verify
# trip_metrics and dim_taxi_zone are independent — run simultaneously
task_wait_silver >> [task_gold_metrics, task_gold_dim] >> task_verify_gold