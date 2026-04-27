# tests/test_transforms.py
# Unit tests for Silver transformation logic
# Tests schema enforcement, type casting, derived columns

import pytest
from chispa import assert_df_equality
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from pyspark.sql.types import (
    IntegerType, DoubleType, StringType, DateType, TimestampType
)
from datetime import datetime, date


# ── Import the function under test ────────────────────────────
# We extract the transform logic into a testable function
# This is why separating logic into functions matters
def apply_silver_schema(df):
    '''
    Apply Silver schema enforcement to Bronze DataFrame.
    This is the exact same logic as in 02_silver_transform Cell 3.
    Extracted here so it can be unit tested.
    '''
    return (
        df.select(
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
        .withColumn('source_year', lit(2023))
        .withColumn('pickup_date', to_date(col('pickup_datetime')))
    )


class TestSilverSchema:
    '''Tests for Silver schema enforcement'''

    def test_column_count(self, spark, sample_bronze_df):
        '''Silver should have exactly 17 columns after transform'''
        result = apply_silver_schema(sample_bronze_df)
        assert len(result.columns) == 17, \
            f'Expected 17 columns, got {len(result.columns)}'

    def test_vendor_id_is_integer(self, spark, sample_bronze_df):
        '''VendorID should be cast from Long to Integer'''
        result = apply_silver_schema(sample_bronze_df)
        dtype = dict(result.dtypes)['vendor_id']
        assert dtype == 'int', f'vendor_id should be int, got {dtype}'

    def test_location_ids_are_integer(self, spark, sample_bronze_df):
        '''PULocationID and DOLocationID should be cast to Integer'''
        result = apply_silver_schema(sample_bronze_df)
        dtypes = dict(result.dtypes)
        assert dtypes['pickup_location_id'] == 'int'
        assert dtypes['dropoff_location_id'] == 'int'

    def test_fare_amount_is_double(self, spark, sample_bronze_df):
        '''fare_amount should remain Double'''
        result = apply_silver_schema(sample_bronze_df)
        dtype = dict(result.dtypes)['fare_amount']
        assert dtype == 'double', f'fare_amount should be double, got {dtype}'

    def test_pickup_date_derived(self, spark, sample_bronze_df):
        '''pickup_date should be derived from pickup_datetime as DateType'''
        result = apply_silver_schema(sample_bronze_df)
        dtype = dict(result.dtypes)['pickup_date']
        assert dtype == 'date', f'pickup_date should be date, got {dtype}'

    def test_source_year_constant(self, spark, sample_bronze_df):
        '''source_year should be 2023 for all rows'''
        result = apply_silver_schema(sample_bronze_df)
        years = result.select('source_year').distinct().collect()
        assert len(years) == 1
        assert years[0]['source_year'] == 2023

    def test_dropped_columns_absent(self, spark, sample_bronze_df):
        '''Columns dropped in Silver should not exist in output'''
        result = apply_silver_schema(sample_bronze_df)
        dropped = ['store_and_fwd_flag', 'extra', 'mta_tax', 'improvement_surcharge']
        for col_name in dropped:
            assert col_name not in result.columns, \
                f'Column {col_name} should have been dropped in Silver'

    def test_row_count_preserved(self, spark, sample_bronze_df):
        '''Schema transform should not drop any rows'''
        result = apply_silver_schema(sample_bronze_df)
        assert result.count() == sample_bronze_df.count(), \
            'Schema transform should not drop rows — DQ handles that'