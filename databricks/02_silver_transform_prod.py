# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports + Config

# COMMAND ----------

# MAGIC %run /Users/swagath.kumar.reddy.kristapati@ctpsandbox.com/nyctaxi_pipeline_prod/pipeline_config

# COMMAND ----------

# Import central config — sets ADLS auth + all path variables
# This one line replaces all hardcoded paths and the account key
 
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date,
    when, count
)
from pyspark.sql.types import (
    IntegerType, DoubleType, StringType, DateType
)
 
print(f'Silver transform starting')
print(f'Source  : {BRONZE_PATH}')
print(f'Target  : {SILVER_PATH}')
print(f'QZone   : {QUARANTINE_PATH}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read all 12 bronze months (handles schema drift)

# COMMAND ----------

# Read each month independently to handle schema drift
# Schema drift: NYC TLC files have type inconsistencies across months
# (e.g. month=10 has INT64 where others have DOUBLE)
# unionByName with allowMissingColumns=True reconciles differences
# Our explicit casts in Cell 3 then normalise all types uniformly
 
dfs = []
for m in SOURCE_MONTHS:
    path = f'{BRONZE_PATH}month={m}/'
    df_m = spark.read.parquet(path)
    dfs.append(df_m)
    print(f'  Loaded month={m}: {df_m.count():,} rows')
 
df_bronze = dfs[0]
for df_m in dfs[1:]:
    df_bronze = df_bronze.unionByName(df_m, allowMissingColumns=True)
 
total = df_bronze.count()
print(f'')
print(f'Total Bronze rows loaded: {total:,}')
print(f'Schema drift handled via unionByName')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema enforcement + type-casting + derived columns

# COMMAND ----------

# Explicit schema enforcement — never trust source data types
# Every column is cast to the expected type
# Columns not needed for Gold are dropped here (store_and_fwd_flag, extra, mta_tax, improvement_surcharge)
# Three derived columns added for downstream use
 
df_typed = (
    df_bronze
    .select(
        col('VendorID').cast(IntegerType()).alias('vendor_id'),
        col('tpep_pickup_datetime').alias('pickup_datetime'),
        col('tpep_dropoff_datetime').alias('dropoff_datetime'),
        col('passenger_count').cast(IntegerType()).alias('passenger_count'),
        col('trip_distance').cast(DoubleType()).alias('trip_distance'),
        col('RatecodeID').cast(IntegerType()).alias('rate_code_id'),
        col('PULocationID').cast(IntegerType()).alias('pickup_location_id'),
        col('DOLocationID').cast(IntegerType()).alias('dropoff_location_id'),
        col('payment_type').cast(IntegerType()).alias('payment_type'),
        col('fare_amount').cast(DoubleType()).alias('fare_amount'),
        col('tip_amount').cast(DoubleType()).alias('tip_amount'),
        col('tolls_amount').cast(DoubleType()).alias('tolls_amount'),
        col('congestion_surcharge').cast(DoubleType()).alias('congestion_surcharge'),
        col('airport_fee').cast(DoubleType()).alias('airport_fee'),
        col('total_amount').cast(DoubleType()).alias('total_amount'),
    )
    # Audit column — when was this record processed
    .withColumn('ingestion_timestamp', current_timestamp())
    # Partition helper and Gold aggregation key
    .withColumn('source_year', lit(SOURCE_YEAR))
    # Derived date — used for partitioning and Gold groupBy
    .withColumn('pickup_date', to_date(col('pickup_datetime')))
)
 
typed_count = df_typed.count()
print(f'Typed rows  : {typed_count:,}')
print(f'Column count: {len(df_typed.columns)}')
df_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data-Quality checks + rejection tagging

# COMMAND ----------

# Tag every row with a rejection reason
# Rules evaluated in PRIORITY ORDER — first match wins
# .otherwise(None) means no rule fired = good record
# Bad records are NOT dropped — they are routed to quarantine
# with the rejection_reason column for audit and replay
 
df_with_dq = df_typed.withColumn(
    'rejection_reason',
    when(col('pickup_location_id').isNull(),  lit('NULL_PICKUP_LOCATION'))
    .when(col('dropoff_location_id').isNull(), lit('NULL_DROPOFF_LOCATION'))
    .when(col('fare_amount').isNull(),          lit('NULL_FARE_AMOUNT'))
    .when(col('trip_distance').isNull(),        lit('NULL_TRIP_DISTANCE'))
    .when(col('fare_amount') <= 0,              lit('INVALID_FARE_AMOUNT'))
    .when(col('trip_distance') < 0,             lit('NEGATIVE_TRIP_DISTANCE'))
    .when(col('total_amount') <= 0,             lit('INVALID_TOTAL_AMOUNT'))
    .when(
        col('pickup_datetime') < lit('2023-01-01').cast('timestamp'),
        lit('PICKUP_OUT_OF_RANGE')
    )
    .when(
        col('pickup_datetime') > lit('2023-12-31').cast('timestamp'),
        lit('PICKUP_OUT_OF_RANGE')
    )
    .otherwise(lit(None))
)
 
# Split good and bad
df_good = df_with_dq.filter(col('rejection_reason').isNull()).drop('rejection_reason')
df_bad  = df_with_dq.filter(col('rejection_reason').isNotNull())
 
good_count  = df_good.count()
bad_count   = df_bad.count()
total_count = df_with_dq.count()
qrate       = bad_count / total_count
 
print(f'Good records  : {good_count:,}')
print(f'Bad records   : {bad_count:,}')
print(f'Quarantine %  : {qrate * 100:.2f}%')
print(f'Threshold     : {DQ_QUARANTINE_THRESHOLD * 100:.0f}%')
 
# Hard fail if quarantine rate exceeds threshold
# This blocks the Gold DAG from running on bad data
if qrate > DQ_QUARANTINE_THRESHOLD:
    raise ValueError(
        f'DQ FAILED: Quarantine rate {qrate*100:.2f}% exceeds '
        f'threshold {DQ_QUARANTINE_THRESHOLD*100:.0f}%. '
        f'Pipeline halted. Investigate rejection reasons.'
    )
 
print(f'DQ check PASSED')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quarantine rejection reason breakdown

# COMMAND ----------

# Show breakdown of rejection reasons
# In production: this would feed a DQ monitoring dashboard
print('Rejection reason breakdown:')
display(
    df_bad
    .groupBy('rejection_reason')
    .count()
    .orderBy('count', ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Quarantine records

# COMMAND ----------

# Write bad records to quarantine as Parquet
# Quarantine does not need Delta — it is a dead-end store
# rejection_reason column is retained for audit
# mode='overwrite' is safe here — full pipeline reruns replace quarantine
 
# Clear existing quarantine (handles .gitkeep placeholder)
try:
    dbutils.fs.rm(QUARANTINE_PATH, recurse=True)
    print('Cleared quarantine path')
except:
    print('Quarantine path already clean')
 
df_bad.write \
    .format('parquet') \
    .mode('overwrite') \
    .save(QUARANTINE_PATH)
 
print(f'Quarantine write complete: {bad_count:,} records')
print(f'Path: {QUARANTINE_PATH}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Silver Delta table

# COMMAND ----------

# Write good records to Silver as Delta
# Partitioned by source_year / pickup_date for query performance
# pickup_date partitioning matches Gold aggregation pattern
 
# Clear path — removes .gitkeep placeholder on first run
try:
    dbutils.fs.rm(SILVER_PATH, recurse=True)
    print('Cleared silver path')
except:
    print('Silver path already clean')
 
df_good.write \
    .format('delta') \
    .mode('overwrite') \
    .partitionBy('source_year', 'pickup_date') \
    .save(SILVER_PATH)
 
print(f'Silver write complete: {good_count:,} rows')
print(f'Path: {SILVER_PATH}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register in unity catalog

# COMMAND ----------

# Register Silver Delta table in Unity Catalog
# hive_metastore is the default catalog — always available
# USING DELTA + LOCATION = external table (data in ADLS, metadata in catalog)
# Dropping this table removes metadata only — data in ADLS is untouched
 
spark.sql(f'CREATE DATABASE IF NOT EXISTS {UC_CATALOG}.{UC_SILVER_DB}')
print(f'Database: {UC_CATALOG}.{UC_SILVER_DB}')
 
spark.sql(f'''
    CREATE TABLE IF NOT EXISTS {UC_SILVER_TABLE}
    USING DELTA
    LOCATION '{SILVER_PATH}'
''')
print(f'Registered: {UC_SILVER_TABLE}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Silver output

# COMMAND ----------

# Read Silver back and verify
# Query by table name (Unity Catalog) not by path
# This confirms end-to-end: write + catalog registration working
 
df_silver_check = spark.read.format('delta').load(SILVER_PATH)
silver_count = df_silver_check.count()
 
print(f'Silver row count  : {silver_count:,}')
print(f'Silver columns    : {len(df_silver_check.columns)}')
print(f'Partitions        : source_year, pickup_date')
 
# Verify via Unity Catalog SQL
result = spark.sql(f'SELECT COUNT(*) as rows FROM {UC_SILVER_TABLE}')
display(result)
 
# Date range check
df_silver_check.selectExpr(
    'MIN(pickup_date) as earliest',
    'MAX(pickup_date) as latest'
).show()
 
# Final row count guard
if silver_count < DQ_MIN_SILVER_ROWS:
    raise ValueError(
        f'Silver row count {silver_count:,} below minimum '
        f'{DQ_MIN_SILVER_ROWS:,}. Pipeline halted.'
    )
 
print(f'Silver validation PASSED')
print(f'Pipeline: Bronze → Silver complete')

# COMMAND ----------

