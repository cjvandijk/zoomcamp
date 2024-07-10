#!/usr/bin/env python
# coding: utf-8

import sys
import os
import boto3


def get_year_month():
    try:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
    except IndexError:
        print("Provide a 4 digit year as first argument, month as second argument")
        sys.exit()

    if (month < 1) or (month > 12):
        print(f"Month must be between 1 and 12, received: {month}")
        sys.exit()

    if year < 1972:
        print(f"Year must be 4 digits; received: {year}")
        sys.exit()

    return year, month


def set_options():
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', None)

    if S3_ENDPOINT_URL:
        options = {
            'client_kwargs': {
                'endpoint_url': S3_ENDPOINT_URL
            }
        }
    else:
        options = None

    return options


def get_trip_data_path(year, month):
    default_data_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    trip_data_pattern = os.getenv('TRIP_DATA_FILE_PATTERN', default_data_pattern)
    return trip_data_pattern.format(year=year, month=month)


def get_test_data_path(bucket, year, month):
    default_data_pattern = "s3://{bucket}/{year:04d}-{month:02d}/test_data.parquet"
    test_data_pattern = os.getenv('TEST_DATA_FILE_PATTERN', default_data_pattern)
    return test_data_pattern.format(bucket=bucket, year=year, month=month)


def get_predictions_file_path(bucket, year, month):
    default_predictions_file_pattern = 's3://{bucket}/{year:04d}-{month:02d}/predictions.parquet'
    predictions_file_pattern = os.getenv('PREDICTIONS_FILE_PATTERN', default_predictions_file_pattern)
    return predictions_file_pattern.format(bucket=bucket, year=year, month=month)


def path_names(year: int, month: int):
    BUCKET = os.getenv("BUCKET", None)
    trip_data_file = get_trip_data_path(year, month)
    test_data_file = get_test_data_path(BUCKET, year, month)
    predictions_file = get_predictions_file_path(BUCKET, year, month)
    return trip_data_file, test_data_file, predictions_file


def upload_to_s3(local_file, bucket, s3_file):
    s3 = boto3.client("s3")
    with open(local_file) as f:
       object_data = f.read()
       s3.upload_file(object_data, bucket, s3_file)

def upload_to_aws(local_file, bucket_name, s3_file):
    s3 = boto3.client("s3")
    """Uploads file to s3 bucket"""
    s3.upload_file(local_file, bucket_name, s3_file)

    

