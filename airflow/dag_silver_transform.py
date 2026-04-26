# dag_silver_transform.py
# ─────────────────────────────────────────────────────────────────
# DAG 2 — Silver Transformation
# Responsibility: Trigger the Databricks Silver notebook after
# Bronze ingestion completes. Applies schema enforcement, DQ checks,
# and writes good records to Silver Delta table.
#
# Depends on: dag_bronze_ingest completing successfully
# In production: uses DatabricksRunNowOperator to trigger a
# Databricks job. Here we simulate with PythonOperator.
# ─────────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
default_args = {
   "owner": "nyctaxi_pipeline",
   "depends_on_past": False,
   "email_on_failure": False,
   "retries": 2,
   "retry_delay": timedelta(minutes=5),
}
dag = DAG(
   dag_id="dag_silver_transform",
   default_args=default_args,
   description="Run Silver transformation after Bronze ingestion",
   schedule="@monthly",
   start_date=datetime(2023, 1, 1),
   catchup=False,
   tags=["nyctaxi", "silver", "transform"],
)
# ── TASK FUNCTIONS ─────────────────────────────────────────────────
def wait_for_bronze(**context):
   """
   In production: ExternalTaskSensor waits for dag_bronze_ingest
   to complete successfully before allowing Silver to proceed.
   This is the cross-DAG dependency — Silver never runs unless
   Bronze succeeded first.
   """
   print("=" * 60)
   print("TASK: wait_for_bronze")
   print("Checking: dag_bronze_ingest completed successfully")
   print("Status: CONFIRMED — Bronze verified 12/12 files")
   print("Proceeding to Silver transformation")
   print("=" * 60)
   return "Bronze dependency satisfied"

def run_silver_notebook(**context):
   """
   In production: triggers 02_silver_transform notebook in
   Databricks via DatabricksRunNowOperator.
   Applies:
     - Schema enforcement + type casting
     - 9 DQ rules
     - Quarantine pattern for bad records
     - Delta write to silver/yellow_taxi/
   """
   print("=" * 60)
   print("TASK: run_silver_notebook")
   print("Databricks workspace : dbw-nyctaxi-prod")
   print("Notebook             : 02_silver_transform")
   print("Cluster              : cluster-nyctaxi-dev")
   print("Expected output      : 37,840,000 good records → Silver Delta")
   print("Expected quarantine  : ~470,226 bad records → quarantine/")
   print("=" * 60)
   # In production:
   # from airflow.providers.databricks.operators.databricks import (
   #     DatabricksRunNowOperator
   # )
   # DatabricksRunNowOperator(
   #     task_id="run_silver_notebook",
   #     databricks_conn_id="databricks_default",
   #     job_id=<<silver_job_id>>,
   # )
   return "Silver notebook triggered"

def verify_silver_quality(**context):
   """
   Verifies Silver output meets quality thresholds:
   - Row count > 0
   - Quarantine rate < 5% (hard fail if exceeded)
   In production: queries hive_metastore.silver.yellow_taxi via
   Databricks SQL or Spark submit.
   """
   print("=" * 60)
   print("TASK: verify_silver_quality")
   # Simulate verification results
   total_rows = 38_310_226
   good_rows = 37_840_000
   bad_rows = 470_226
   quarantine_rate = (bad_rows / total_rows) * 100
   print(f"Total Bronze rows  : {total_rows:,}")
   print(f"Good rows → Silver : {good_rows:,}")
   print(f"Bad rows → Quarant : {bad_rows:,}")
   print(f"Quarantine rate    : {quarantine_rate:.2f}%")
   # Hard fail if quarantine rate exceeds 5%
   if quarantine_rate > 5.0:
       raise ValueError(
           f"Quarantine rate {quarantine_rate:.2f}% exceeds 5% threshold. "
           f"Pipeline halted — investigate data quality issues."
       )
   print(f"Quality check PASSED — {quarantine_rate:.2f}% < 5% threshold")
   print("=" * 60)
   return "Silver quality verified"

# ── TASK DEFINITIONS ───────────────────────────────────────────────
task_wait_bronze = PythonOperator(
   task_id="wait_for_bronze",
   python_callable=wait_for_bronze,
   dag=dag,
)
task_run_silver = PythonOperator(
   task_id="run_silver_notebook",
   python_callable=run_silver_notebook,
   dag=dag,
)
task_verify_silver = PythonOperator(
   task_id="verify_silver_quality",
   python_callable=verify_silver_quality,
   dag=dag,
)
# ── TASK DEPENDENCIES ──────────────────────────────────────────────
# wait → run notebook → verify quality
# If any step fails, downstream tasks don't run
task_wait_bronze >> task_run_silver >> task_verify_silver