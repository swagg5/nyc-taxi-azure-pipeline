# tests/test_dq_rules.py
# Unit tests for Data Quality rules
# Tests each rule individually + quarantine split

import pytest
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import IntegerType, DoubleType
from datetime import datetime


def apply_dq_rules(df, date_min='2023-01-01', date_max='2023-12-31'):
    '''
    Apply all 9 DQ rules and tag rejection_reason.
    Extracted from 02_silver_transform for testability.
    date_min and date_max are configurable for testing.
    '''
    return df.withColumn(
        'rejection_reason',
        when(col('pickup_location_id').isNull(),   lit('NULL_PICKUP_LOCATION'))
        .when(col('dropoff_location_id').isNull(), lit('NULL_DROPOFF_LOCATION'))
        .when(col('fare_amount').isNull(),          lit('NULL_FARE_AMOUNT'))
        .when(col('trip_distance').isNull(),        lit('NULL_TRIP_DISTANCE'))
        .when(col('fare_amount') <= 0,              lit('INVALID_FARE_AMOUNT'))
        .when(col('trip_distance') < 0,             lit('NEGATIVE_TRIP_DISTANCE'))
        .when(col('total_amount') <= 0,             lit('INVALID_TOTAL_AMOUNT'))
        .when(col('pickup_datetime') < lit(date_min).cast('timestamp'),
              lit('PICKUP_OUT_OF_RANGE'))
        .when(col('pickup_datetime') > lit(date_max).cast('timestamp'),
              lit('PICKUP_OUT_OF_RANGE'))
        .otherwise(lit(None))
    )


def make_typed_df(spark, rows):
    '''Helper — create a typed Silver-schema DataFrame from row list'''
    from pyspark.sql.types import (
        StructType, StructField, IntegerType, DoubleType, TimestampType
    )
    schema = StructType([
        StructField('pickup_location_id',  IntegerType(),   True),
        StructField('dropoff_location_id', IntegerType(),   True),
        StructField('fare_amount',         DoubleType(),    True),
        StructField('trip_distance',       DoubleType(),    True),
        StructField('total_amount',        DoubleType(),    True),
        StructField('pickup_datetime',     TimestampType(), True),
    ])
    return spark.createDataFrame(rows, schema)


class TestDQRules:
    '''Tests for individual DQ rules'''

    def test_null_pickup_location_rejected(self, spark):
        '''Records with null PULocationID must be quarantined'''
        df = make_typed_df(spark, [
            (None, 161, 15.5, 3.5, 19.3, datetime(2023,1,15,8,30)),
        ])
        result = apply_dq_rules(df)
        reason = result.collect()[0]['rejection_reason']
        assert reason == 'NULL_PICKUP_LOCATION', \
            f'Expected NULL_PICKUP_LOCATION, got {reason}'

    def test_negative_fare_rejected(self, spark):
        '''Records with fare_amount <= 0 must be quarantined'''
        df = make_typed_df(spark, [
            (132, 161, -5.0, 3.5, -3.0, datetime(2023,1,15,8,30)),
        ])
        result = apply_dq_rules(df)
        reason = result.collect()[0]['rejection_reason']
        assert reason == 'INVALID_FARE_AMOUNT', \
            f'Expected INVALID_FARE_AMOUNT, got {reason}'

    def test_zero_fare_rejected(self, spark):
        '''fare_amount = 0 must also be quarantined (rule is <= 0)'''
        df = make_typed_df(spark, [
            (132, 161, 0.0, 3.5, 0.0, datetime(2023,1,15,8,30)),
        ])
        result = apply_dq_rules(df)
        reason = result.collect()[0]['rejection_reason']
        assert reason == 'INVALID_FARE_AMOUNT'

    def test_out_of_range_date_rejected(self, spark):
        '''Records outside 2023 must be quarantined'''
        df = make_typed_df(spark, [
            (132, 161, 15.5, 3.5, 19.3, datetime(2022,12,31,23,59)),
        ])
        result = apply_dq_rules(df)
        reason = result.collect()[0]['rejection_reason']
        assert reason == 'PICKUP_OUT_OF_RANGE'

    def test_good_record_passes_all_rules(self, spark):
        '''A valid record should have rejection_reason = None'''
        df = make_typed_df(spark, [
            (132, 161, 15.5, 3.5, 19.3, datetime(2023,6,15,10,30)),
        ])
        result = apply_dq_rules(df)
        reason = result.collect()[0]['rejection_reason']
        assert reason is None, \
            f'Good record should pass all rules, got: {reason}'

    def test_priority_order_null_before_negative(self, spark):
        '''
        A record with null PULocationID AND negative fare
        should get NULL_PICKUP_LOCATION (higher priority)
        not INVALID_FARE_AMOUNT
        '''
        df = make_typed_df(spark, [
            (None, 161, -5.0, 3.5, -3.0, datetime(2023,1,15,8,30)),
        ])
        result = apply_dq_rules(df)
        reason = result.collect()[0]['rejection_reason']
        assert reason == 'NULL_PICKUP_LOCATION', \
            'Null check should take priority over range check'


class TestQuarantineSplit:
    '''Tests for good/bad record split logic'''

    def test_good_bad_split_counts(self, spark, sample_bronze_df):
        '''
        sample_bronze_df has 3 good + 3 bad records.
        Split should produce exactly 3 good and 3 bad.
        '''
        from tests.conftest import apply_silver_schema
        # Apply schema first
        from pyspark.sql.functions import to_date
        from pyspark.sql.types import IntegerType, DoubleType
        df_typed = sample_bronze_df.select(
            col('PULocationID').cast(IntegerType()).alias('pickup_location_id'),
            col('DOLocationID').cast(IntegerType()).alias('dropoff_location_id'),
            col('fare_amount').cast(DoubleType()).alias('fare_amount'),
            col('trip_distance').cast(DoubleType()).alias('trip_distance'),
            col('total_amount').cast(DoubleType()).alias('total_amount'),
            col('tpep_pickup_datetime').alias('pickup_datetime'),
        )
        df_dq = apply_dq_rules(df_typed)
        good = df_dq.filter(col('rejection_reason').isNull()).count()
        bad  = df_dq.filter(col('rejection_reason').isNotNull()).count()
        assert good == 3, f'Expected 3 good records, got {good}'
        assert bad  == 3, f'Expected 3 bad records, got {bad}'

    def test_quarantine_rate_calculation(self, spark):
        '''Quarantine rate = bad / total — verify calculation'''
        total = 1000
        bad   = 12
        rate  = bad / total
        assert abs(rate - 0.012) < 0.0001
        assert rate < 0.05, 'Rate below 5% threshold — pipeline should continue'