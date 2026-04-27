# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports + config

# COMMAND ----------

# MAGIC %run /Users/swagath.kumar.reddy.kristapati@ctpsandbox.com/nyctaxi_pipeline_prod/pipeline_config

# COMMAND ----------

# Import central config — sets ADLS auth + all path variables
 
from pyspark.sql.functions import col, count, when, isnan
 
print(f'Bronze validation starting')
print(f'Reading from: {BRONZE_PATH}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read one month and validate schema

# COMMAND ----------

# Read January 2023 for schema validation
# We validate one month first — faster than reading all 12
sample_path = f'{BRONZE_PATH}month=01/'
df_sample = spark.read.parquet(sample_path)
 
print(f'Row count  : {df_sample.count():,}')
print(f'Column count: {len(df_sample.columns)}')
df_sample.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Null check on key columns

# COMMAND ----------

key_cols = [
    'tpep_pickup_datetime', 'tpep_dropoff_datetime',
    'trip_distance', 'fare_amount',
    'PULocationID', 'DOLocationID', 'passenger_count'
]
 
null_counts = df_sample.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in key_cols
])
 
print('Null counts per key column:')
display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate all 12 months exist

# COMMAND ----------

# Confirm all 12 month partitions exist in Bronze
# Fail with clear message if any month is missing
from pyspark.sql.functions import input_file_name
 
missing_months = []
for month in SOURCE_MONTHS:
    path = f'{BRONZE_PATH}month={month}/'
    try:
        count = spark.read.parquet(path).count()
        print(f'  month={month}: {count:,} rows  OK')
    except Exception as e:
        missing_months.append(month)
        print(f'  month={month}: MISSING  FAIL')
 
if missing_months:
    raise ValueError(
        f'Bronze validation FAILED. Missing months: {missing_months}'
    )
else:
    print(f'Bronze validation PASSED — all 12 months present')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preview sample data

# COMMAND ----------

print('Sample rows from Bronze (month=01):')
display(df_sample.limit(10))

# COMMAND ----------

