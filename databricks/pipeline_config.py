# Databricks notebook source
# MAGIC %md
# MAGIC ### Complete pipeline config notebook (single-cell)

# COMMAND ----------

# ================================================================
# pipeline_config
# NYC Yellow Taxi Pipeline — Central Configuration
# ================================================================
# ALL environment-specific values live here.
# Every notebook imports this via: %run ./pipeline_config
# To change environment: edit this file only.
# To go to production: replace ACCOUNT_KEY with Key Vault call.
# ================================================================
 
# ── ENVIRONMENT ─────────────────────────────────────────────────
ENV             = 'dev'            # dev | staging | prod
PIPELINE_NAME   = 'nyctaxi'
PIPELINE_VERSION = 'v1.0'
 
# ── STORAGE ──────────────────────────────────────────────────────
STORAGE_ACCOUNT = 'adlsnyctaxiprod'
CONTAINER       = 'nyctaxi'
 
# ── ACCOUNT KEY ──────────────────────────────────────────────────
# DEV: replace the string below with your actual ADLS key
# PROD: replace this entire line with:
#   ACCOUNT_KEY = dbutils.secrets.get(
#       scope='nyctaxi-kv-scope', key='adls-account-key')
ACCOUNT_KEY = 'REPLACE_WIH_YOUR_KEY'
 
# ── SPARK AUTHENTICATION ─────────────────────────────────────────
# Set Spark config so all read/write calls authenticate automatically
spark.conf.set(
    f'fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net',
    ACCOUNT_KEY
)
 
# ── BASE PATH ────────────────────────────────────────────────────
_BASE = f'abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net'
 
# ── LAYER PATHS ──────────────────────────────────────────────────
BRONZE_PATH     = f'{_BASE}/bronze/yellow_taxi/year=2023/'
SILVER_PATH     = f'{_BASE}/silver/yellow_taxi/'
GOLD_METRICS    = f'{_BASE}/gold/trip_metrics/'
GOLD_DIM_ZONE   = f'{_BASE}/gold/dim_taxi_zone/'
QUARANTINE_PATH = f'{_BASE}/quarantine/yellow_taxi/'
 
# ── SOURCE CONFIG ────────────────────────────────────────────────
TLC_BASE_URL    = 'https://d37ci6vzurychx.cloudfront.net'
SOURCE_YEAR     = 2023
SOURCE_MONTHS   = [f'{m:02d}' for m in range(1, 13)]
 
# ── DATA QUALITY THRESHOLDS ──────────────────────────────────────
DQ_QUARANTINE_THRESHOLD = 0.05     # Fail pipeline if > 5% quarantined
DQ_MIN_SILVER_ROWS      = 1_000_000 # Fail pipeline if Silver < 1M rows
DQ_MIN_GOLD_ROWS        = 1_000    # Fail pipeline if Gold < 1K rows
 
# ── UNITY CATALOG ────────────────────────────────────────────────
UC_CATALOG       = 'hive_metastore'
UC_SILVER_DB     = 'silver'
UC_GOLD_DB       = 'gold'
UC_SILVER_TABLE  = f'{UC_CATALOG}.{UC_SILVER_DB}.yellow_taxi'
UC_METRICS_TABLE = f'{UC_CATALOG}.{UC_GOLD_DB}.trip_metrics'
UC_DIM_TABLE     = f'{UC_CATALOG}.{UC_GOLD_DB}.dim_taxi_zone'
 
# ── PRINT SUMMARY ────────────────────────────────────────────────
print('=' * 55)
print(f'Pipeline : {PIPELINE_NAME} {PIPELINE_VERSION}')
print(f'Env      : {ENV}')
print(f'Storage  : {STORAGE_ACCOUNT}/{CONTAINER}')
print(f'Bronze   : {BRONZE_PATH}')
print(f'Silver   : {SILVER_PATH}')
print(f'Gold     : {GOLD_METRICS}')
print(f'Gold Dim : {GOLD_DIM_ZONE}')
print(f'UC Silver: {UC_SILVER_TABLE}')
print('=' * 55)