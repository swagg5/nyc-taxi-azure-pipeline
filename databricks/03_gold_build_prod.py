# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports + Configs

# COMMAND ----------

# MAGIC %run /Users/swagath.kumar.reddy.kristapati@ctpsandbox.com/nyctaxi_pipeline_prod/pipeline_config

# COMMAND ----------

# Import central config — sets ADLS auth + all path variables
 
from pyspark.sql.functions import (
    col, lit, count, avg, sum as spark_sum,
    current_date, current_timestamp, to_date,
    when, max as spark_max
)
from pyspark.sql.types import (
    IntegerType, DoubleType, StringType, DateType
)
from delta.tables import DeltaTable
import pandas as pd
 
print(f'Gold build starting')
print(f'Source  : {SILVER_PATH}')
print(f'Target 1: {GOLD_METRICS}')
print(f'Target 2: {GOLD_DIM_ZONE}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-flight: Clear Gold paths + register database

# COMMAND ----------

# Clear Gold paths — removes .gitkeep on first run
# On subsequent runs: Gold tables already exist, clear is a no-op
# Delta will re-initialise on first write
 
for path, name in [
    (GOLD_METRICS, 'gold/trip_metrics'),
    (GOLD_DIM_ZONE, 'gold/dim_taxi_zone')
]:
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f'Cleared: {name}')
    except:
        print(f'Already clean: {name}')
 
# Register Gold database in Unity Catalog
spark.sql(f'CREATE DATABASE IF NOT EXISTS {UC_CATALOG}.{UC_GOLD_DB}')
print(f'Database ready: {UC_CATALOG}.{UC_GOLD_DB}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Silver + Sanity check

# COMMAND ----------

# Read Silver Delta table
# Always read via Delta format — gets latest committed version
df_silver = spark.read.format('delta').load(SILVER_PATH)
 
silver_count = df_silver.count()
print(f'Silver rows: {silver_count:,}')
 
# Guard: fail Gold if Silver is empty or below threshold
if silver_count < DQ_MIN_SILVER_ROWS:
    raise ValueError(
        f'Silver count {silver_count:,} below minimum. '
        f'Run Silver notebook first.'
    )
 
# Confirm date range
df_silver.selectExpr(
    'MIN(pickup_date) as earliest',
    'MAX(pickup_date) as latest'
).show()
 
print('Silver sanity check PASSED')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build trip metrics aggregation

# COMMAND ----------

# Aggregate Silver by pickup_date + pickup_location_id
# One row per zone per day — this is the Gold analytical table
# Six KPI columns computed:
#   total_trips     : how many trips from this zone on this day
#   avg_fare        : average base fare
#   avg_trip_distance: average trip length in miles
#   total_revenue   : sum of total_amount (all charges + tips)
#   avg_tip         : average tip — proxy for zone affluence
#   avg_passenger_count: average occupancy
 
df_trip_metrics = (
    df_silver
    .groupBy('pickup_date', 'pickup_location_id')
    .agg(
        count('*').cast(IntegerType()).alias('total_trips'),
        avg('fare_amount').alias('avg_fare'),
        avg('trip_distance').alias('avg_trip_distance'),
        spark_sum('total_amount').alias('total_revenue'),
        avg('tip_amount').alias('avg_tip'),
        avg('passenger_count').alias('avg_passenger_count')
    )
    .withColumn('ingestion_timestamp', current_timestamp())
)
 
metrics_count = df_trip_metrics.count()
print(f'trip_metrics rows   : {metrics_count:,}')
print(f'trip_metrics columns: {len(df_trip_metrics.columns)}')
display(df_trip_metrics.orderBy('pickup_date').limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write trip metrics (first load overwrite, then MERGE)

# COMMAND ----------

# LOAD STRATEGY:
#   First run  : overwrite — table does not exist yet
#   Subsequent : Delta MERGE — insert new, update changed, leave unchanged
 
# Check if table already exists
try:
    existing = spark.read.format('delta').load(GOLD_METRICS)
    table_exists = True
    print(f'Existing trip_metrics: {existing.count():,} rows — will MERGE')
except:
    table_exists = False
    print('No existing trip_metrics — will overwrite (first load)')
 
if not table_exists:
    # First load — write fresh
    df_trip_metrics.write \
        .format('delta') \
        .mode('overwrite') \
        .partitionBy('pickup_date') \
        .save(GOLD_METRICS)
    print('First load complete — overwrite')
else:
    # Subsequent loads — MERGE
    # Business key: pickup_date + pickup_location_id
    # whenMatchedUpdateAll: update all columns if match found
    # whenNotMatchedInsertAll: insert if new zone-day combination
    target = DeltaTable.forPath(spark, GOLD_METRICS)
    (
        target.alias('target')
        .merge(
            df_trip_metrics.alias('source'),
            'target.pickup_date = source.pickup_date AND '
            'target.pickup_location_id = source.pickup_location_id'
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print('MERGE complete')
 
# Register in Unity Catalog
spark.sql(f'''
    CREATE TABLE IF NOT EXISTS {UC_METRICS_TABLE}
    USING DELTA
    LOCATION '{GOLD_METRICS}'
''')
print(f'Registered: {UC_METRICS_TABLE}')
 
# Show Delta transaction history
spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_METRICS}`") \
    .select('version', 'timestamp', 'operation') \
    .show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build dim taxi zone (download CSV + SCD Type 2)

# COMMAND ----------

# Download NYC TLC taxi zone lookup CSV — 265 zones
# This is a static reference file — changes slowly
# In production: store in ADLS as a managed reference file
# rather than downloading from URL on each run
 
from pyspark.sql.functions import monotonically_increasing_id
 
taxi_zone_url = f'{TLC_BASE_URL}/misc/taxi_zone_lookup.csv'
pdf_zones = pd.read_csv(taxi_zone_url)
print(f'Zones downloaded: {len(pdf_zones)} rows')
 
df_zones_raw = spark.createDataFrame(pdf_zones)
 
# Build SCD Type 2 dimension — initial version
# All records start as current (is_current=True, effective_end_date=None)
df_dim_zone = (
    df_zones_raw
    .select(
        col('LocationID').cast(IntegerType()).alias('location_id'),
        col('Borough').cast(StringType()).alias('borough'),
        col('Zone').cast(StringType()).alias('zone_name'),
        col('service_zone').cast(StringType()).alias('service_zone')
    )
    # SCD Type 2 control columns
    .withColumn('effective_start_date', current_date())
    .withColumn('effective_end_date',   lit(None).cast(DateType()))
    .withColumn('is_current',            lit(True))
    .withColumn('ingestion_timestamp',   current_timestamp())
)
 
# Add surrogate key — unique per row version
# monotonically_increasing_id: unique longs, not sequential
# Acceptable for surrogate keys — uniqueness is what matters
df_dim_zone = df_dim_zone.withColumn(
    'zone_sk', monotonically_increasing_id().cast(IntegerType())
)
 
print(f'dim_taxi_zone rows   : {df_dim_zone.count()}')
print(f'dim_taxi_zone columns: {len(df_dim_zone.columns)}')
df_dim_zone.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD Type 2 MERGE for dim taxi zone

# COMMAND ----------

# SCD Type 2 MERGE — two step process
#
# STEP 1: Close records where attributes have changed
#   Match: same location_id, currently active (is_current=True),
#          AND at least one attribute is different
#   Action: set is_current=False, effective_end_date=today
#
# STEP 2: Insert new versions for changed zones
#   Find source rows with NO current match in target (left_anti join)
#   Insert with effective_start_date=today, effective_end_date=None, is_current=True
#
# WHY TWO STEPS: Delta MERGE cannot both update old AND insert new
# in a single pass for SCD Type 2. Two operations is the standard pattern.
 
# Check if dim table already exists
try:
    existing_dim = spark.read.format('delta').load(GOLD_DIM_ZONE)
    dim_exists = True
    print(f'Existing dim: {existing_dim.count()} rows — will SCD MERGE')
except:
    dim_exists = False
    print('No existing dim — first load')
 
if not dim_exists:
    # First load — write all 265 zones as current
    df_dim_zone.write \
        .format('delta') \
        .mode('overwrite') \
        .save(GOLD_DIM_ZONE)
    print('dim_taxi_zone first load complete')
else:
    # SCD Type 2 MERGE
    target_dim = DeltaTable.forPath(spark, GOLD_DIM_ZONE)
 
    # STEP 1 — Close changed records
    (
        target_dim.alias('target')
        .merge(
            df_dim_zone.alias('source'),
            '''target.location_id = source.location_id
               AND target.is_current = true
               AND (
                   target.borough      != source.borough      OR
                   target.zone_name    != source.zone_name    OR
                   target.service_zone != source.service_zone
               )'''
        )
        .whenMatchedUpdate(set={
            'is_current':         'false',
            'effective_end_date': 'current_date()'
        })
        .execute()
    )
    print('Step 1 complete — closed changed records')
 
    # STEP 2 — Insert new versions
    # left_anti: source rows with NO current match in target
    df_new_versions = df_dim_zone.alias('source').join(
        target_dim.toDF().filter(col('is_current') == True).alias('target'),
        on='location_id',
        how='left_anti'
    ).select(
        col('source.location_id'),
        col('source.borough'),
        col('source.zone_name'),
        col('source.service_zone'),
        current_date().alias('effective_start_date'),
        lit(None).cast(DateType()).alias('effective_end_date'),
        lit(True).alias('is_current'),
        current_timestamp().alias('ingestion_timestamp')
    ).withColumn(
        'zone_sk', monotonically_increasing_id().cast(IntegerType())
    )
 
    new_count = df_new_versions.count()
    if new_count > 0:
        df_new_versions.write \
            .format('delta') \
            .mode('append') \
            .save(GOLD_DIM_ZONE)
        print(f'Step 2 complete — inserted {new_count} new versions')
    else:
        print('Step 2: no changed zones — nothing to insert')
 
# Register in Unity Catalog
spark.sql(f'''
    CREATE TABLE IF NOT EXISTS {UC_DIM_TABLE}
    USING DELTA
    LOCATION '{GOLD_DIM_ZONE}'
''')
print(f'Registered: {UC_DIM_TABLE}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Gold output

# COMMAND ----------

# Verify both Gold tables have data
df_metrics_check = spark.read.format('delta').load(GOLD_METRICS)
df_dim_check     = spark.read.format('delta').load(GOLD_DIM_ZONE)
 
metrics_rows = df_metrics_check.count()
dim_rows     = df_dim_check.count()
current_rows = df_dim_check.filter(col('is_current') == True).count()
closed_rows  = df_dim_check.filter(col('is_current') == False).count()
 
print(f'trip_metrics rows    : {metrics_rows:,}')
print(f'dim_taxi_zone rows   : {dim_rows:,}')
print(f'  Current records    : {current_rows:,}')
print(f'  Closed records     : {closed_rows:,}')
 
# Guard: fail if Gold tables are empty
if metrics_rows < DQ_MIN_GOLD_ROWS:
    raise ValueError(f'trip_metrics empty or below threshold')
if dim_rows == 0:
    raise ValueError(f'dim_taxi_zone is empty')
 
print('Gold verification PASSED')

# COMMAND ----------

# MAGIC %md
# MAGIC ### End-to-end Verification join

# COMMAND ----------

# Join trip_metrics to dim_taxi_zone — the final analytical output
# Always filter dim on is_current=True before joining
# Without this filter: fan-out — each fact row joins to all historical
# dimension versions, silently multiplying row count
 
df_gold_final = (
    df_metrics_check
    .join(
        df_dim_check.filter(col('is_current') == True),
        df_metrics_check.pickup_location_id == df_dim_check.location_id,
        'left'
    )
    .select(
        df_metrics_check.pickup_date,
        df_dim_check.borough,
        df_dim_check.zone_name,
        df_metrics_check.total_trips,
        df_metrics_check.avg_fare,
        df_metrics_check.total_revenue
    )
    .orderBy(col('total_revenue').desc())
)
 
print(f'End-to-end join rows: {df_gold_final.count():,}')
print('Top 10 zones by total revenue across 2023:')
display(df_gold_final.limit(10))
 
print()
print('=' * 55)
print('PIPELINE COMPLETE: Bronze → Silver → Gold')
print(f'Silver : {silver_count:,} rows')
print(f'Gold M : {metrics_rows:,} rows')
print(f'Gold D : {dim_rows:,} rows ({current_rows} current)')
print('=' * 55)

# COMMAND ----------

