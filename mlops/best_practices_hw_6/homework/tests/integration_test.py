#!/usr/bin/env python
# coding: utf-8

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import os
from datetime import datetime
from deepdiff import DeepDiff
import pandas as pd
import awswrangler as wr

from utils.helpers import set_options, get_predictions_file_path

year = 2023
month = 1
BUCKET = os.getenv("BUCKET", "nyc-duration-claudia")
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', None)
TEST_DATA_FILE_PATTERN = os.getenv(
    "TEST_DATA_FILE_PATTERN",
    "s3://{bucket}/{year:04d}-{month:02d}/test_data.parquet"
    )

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def create_test_dataframe():
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    return pd.DataFrame(data, columns=columns)


def write_test_dataframe(df, output_filename):
    print("*" * 80)
    print("Writing test dataframe to", output_filename)
    options = set_options()
    print("OPTIONS=", options)
    print("output filename", output_filename)

    if options:
        print("Using localstack")
        df.to_parquet(
            output_filename,
            engine='pyarrow',
            compression=None,
            index=False,
            storage_options=options
        )
    else:
        wr.s3.to_parquet(
            df, 
            path=output_filename, 
            index=False
            )


def process_with_batch_script():
    cmd = f"python ../apps/batch.py {year} {month}"
    print("Running command", cmd)
    os.system(cmd)


def read_data(filename: str):
    print("Reading data from", filename)

    options = set_options()

    if options:
        df = pd.read_parquet(
            filename,
            storage_options=options
        )
    else:
        df = wr.s3.read_parquet(filename)  #, dataset=True)

    return df


if __name__ == "__main__":
    test_data_df = create_test_dataframe()

    print("Test dataframe created, writing to",TEST_DATA_FILE_PATTERN.format(bucket=BUCKET, year=year, month=month))
    write_test_dataframe(
        test_data_df, 
        TEST_DATA_FILE_PATTERN.format(bucket=BUCKET, year=year, month=month))

    process_with_batch_script()

    # Read processed data back to validate content
    predictions_filename = get_predictions_file_path(BUCKET, year, month)
    processed_df = read_data(predictions_filename)

    actual_result = processed_df.to_dict(orient="records")

    expected_result = [
        {
            'ride_id': '2023/01_0', 
            'predicted_duration': 23.19714924577506
        }, {
            'ride_id': '2023/01_1', 
            'predicted_duration': 13.08010120625567
        }
        ]

    diff = DeepDiff(expected_result, actual_result, significant_digits=1)

    assert len(diff) == 0

    print("Expected and actual result match.")
    print(processed_df.predicted_duration.sum())

