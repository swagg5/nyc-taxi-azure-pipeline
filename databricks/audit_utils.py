# databricks/audit_utils.py
# Audit table helper functions
# Import in notebooks: %run ./audit_utils

from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col


def write_audit_record(spark, audit_path, run_id, pipeline_name,
                       process_year, process_month, layer,
                       status, metrics: dict):
    '''
    Write one record to the pipeline audit log.
    Call at start (status=started) and end (status=completed/failed).

    Args:
        spark:          Active SparkSession
        audit_path:     ADLS path for audit Delta table
        run_id:         Unique run identifier
        pipeline_name:  e.g. 'nyctaxi'
        process_year:   e.g. '2024'
        process_month:  e.g. '01'
        layer:          bronze | silver | gold
        status:         started | completed | failed
        metrics:        dict with source_rows, good_rows etc.
    '''
    record = Row(
        run_id         = run_id,
        pipeline_name  = pipeline_name,
        process_year   = int(process_year),
        process_month  = process_month,
        layer          = layer,
        status         = status,
        run_timestamp  = datetime.now(),
        source_rows    = int(metrics.get('source_rows',    0)),
        good_rows      = int(metrics.get('good_rows',      0)),
        quarantine_rows= int(metrics.get('quarantine_rows',0)),
        quarantine_rate= float(metrics.get('quarantine_rate',0.0)),
        target_rows    = int(metrics.get('target_rows',    0)),
        duration_secs  = int(metrics.get('duration_secs',  0)),
        error_message  = metrics.get('error_message', None)
    )
    df = spark.createDataFrame([record])
    df.write.format('delta').mode('append').save(audit_path)
    print(f'Audit record written: {run_id} | {layer} | {status}')


def get_last_successful_run(spark, audit_path, layer='silver'):
    '''
    Query audit table for last successfully completed run.
    Returns (year, month) tuple or None if no successful run exists.
    Used by pipeline to determine what has already been processed.
    '''
    try:
        df = spark.read.format('delta').load(audit_path)
        result = (
            df
            .filter(
                (col('status') == 'completed') &
                (col('layer') == layer)
            )
            .orderBy('run_timestamp', ascending=False)
            .limit(1)
            .select('process_year', 'process_month')
            .collect()
        )
        if result:
            return str(result[0]['process_year']), result[0]['process_month']
        return None
    except Exception:
        return None  # First run — audit table not yet created


def get_quarantine_trend(spark, audit_path, last_n_months=6):
    '''
    Return quarantine rate trend for last N months.
    Use this to detect upstream data quality degradation.
    A sudden spike signals source schema change or feed issue.
    '''
    df = spark.read.format('delta').load(audit_path)
    return (
        df
        .filter(
            (col('layer') == 'silver') &
            (col('status') == 'completed')
        )
        .orderBy('run_timestamp', ascending=False)
        .limit(last_n_months)
        .select('process_year', 'process_month', 'quarantine_rate',
                'good_rows', 'quarantine_rows', 'run_timestamp')
        .orderBy('process_year', 'process_month')
    )