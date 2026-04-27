# tests/conftest.py
# Shared pytest fixtures for all test files
# SparkSession is created once per test session for efficiency

import pytest
from pyspark.sql import SparkSession
from datetime import date


@pytest.fixture(scope='session')
def spark():
    '''
    Create a local SparkSession for testing.
    scope=session means one SparkSession for all tests — faster.
    Delta Lake extension enables Delta format in local tests.
    '''
    spark = (
        SparkSession.builder
        .master('local[2]')         # 2 local threads — fast enough for tests
        .appName('nyctaxi-tests')
        .config('spark.sql.extensions',
                'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog',
                'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.shuffle.partitions', '2')  # Fast for small test data
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('ERROR')  # Suppress verbose Spark logs
    yield spark
    spark.stop()


@pytest.fixture
def sample_bronze_df(spark):
    '''
    A small representative Bronze DataFrame for testing.
    Contains both good records and records that should fail DQ.
    Mirrors the real NYC TLC schema exactly.
    '''
    from pyspark.sql.types import (
        StructType, StructField, LongType, DoubleType,
        StringType, TimestampType
    )
    from datetime import datetime

    schema = StructType([
        StructField('VendorID',              LongType(),      True),
        StructField('tpep_pickup_datetime',  TimestampType(), True),
        StructField('tpep_dropoff_datetime', TimestampType(), True),
        StructField('passenger_count',       DoubleType(),    True),
        StructField('trip_distance',         DoubleType(),    True),
        StructField('RatecodeID',            DoubleType(),    True),
        StructField('store_and_fwd_flag',    StringType(),    True),
        StructField('PULocationID',          LongType(),      True),
        StructField('DOLocationID',          LongType(),      True),
        StructField('payment_type',          LongType(),      True),
        StructField('fare_amount',           DoubleType(),    True),
        StructField('extra',                 DoubleType(),    True),
        StructField('mta_tax',               DoubleType(),    True),
        StructField('tip_amount',            DoubleType(),    True),
        StructField('tolls_amount',          DoubleType(),    True),
        StructField('improvement_surcharge', DoubleType(),    True),
        StructField('total_amount',          DoubleType(),    True),
        StructField('congestion_surcharge',  DoubleType(),    True),
        StructField('airport_fee',           DoubleType(),    True),
    ])

    data = [
        # GOOD records
        (2, datetime(2023,1,15,8,30), datetime(2023,1,15,8,45), 1.0, 3.5,  1.0, 'N', 132, 161, 1, 15.5, 0.5, 0.5, 2.0, 0.0, 0.3, 19.3, 2.5, 0.0),
        (1, datetime(2023,1,15,9,0),  datetime(2023,1,15,9,20), 2.0, 5.2,  1.0, 'N', 48,  79,  2, 20.0, 1.0, 0.5, 0.0, 0.0, 0.3, 22.3, 2.5, 0.0),
        (2, datetime(2023,6,10,14,0), datetime(2023,6,10,14,30),1.0, 8.1,  1.0, 'Y', 138, 264, 1, 35.0, 0.0, 0.5, 7.0, 0.0, 0.3, 45.3, 0.0, 1.75),
        # BAD records — should be quarantined
        (2, datetime(2023,1,15,8,30), datetime(2023,1,15,8,45), 1.0, 3.5,  1.0, 'N', None,161, 1, 15.5, 0.5, 0.5, 2.0, 0.0, 0.3, 19.3, 2.5, 0.0),  # NULL PULocationID
        (1, datetime(2023,1,15,9,0),  datetime(2023,1,15,9,20), 1.0, 2.0,  1.0, 'N', 48,  79,  1, -5.0, 0.0, 0.5, 0.0, 0.0, 0.3, -3.0, 0.0, 0.0),  # Negative fare
        (2, datetime(2022,12,31,23,0),datetime(2023,1,1,0,0),  1.0, 1.0,  1.0, 'N', 132, 161, 1, 10.0, 0.0, 0.5, 0.0, 0.0, 0.3, 11.3, 0.0, 0.0),  # Out of date range
    ]

    return spark.createDataFrame(data, schema)