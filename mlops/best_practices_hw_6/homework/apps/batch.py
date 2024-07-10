#!/usr/bin/env python
# coding: utf-8

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from typing import List
import os

import pickle
import pandas as pd
import awswrangler as wr

from utils.helpers import get_year_month, set_options, path_names

"""
This script reads NYC Taxi trip data based on year and month input as arguments. 
1. Data is read into a dataframe, and then prepared for predictions.
2. Durations are predicted for each row in the dataframe using model.bin.
3. A new dataframe is created containing just ride_id and the predicted duration.
4. This dataframe is then written out to the PREDICTIONS_FILE_PATTERN file.

If the S3_ENDPOINT_URL is absent, data will be written to s3. 
Setting the S3_ENDPOINT_URL to "--endpoint-url=http://localhost:4566" 
will cause the file to be written to localstack-s3 for testing purposes.
"""

BUCKET = os.getenv("BUCKET", "nyc-duration-claudia")
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', None)
PREDICTIONS_FILE_PATTERN = os.getenv('PREDICTIONS_FILE_PATTERN', None)
TRIP_DATA_FILE_PATTERN = os.getenv('TRIP_DATA_FILE_PATTERN', None)
TEST_DATA_FILE_PATTERN = os.getenv('TEST_DATA_FILE_PATTERN', None)

    
def read_data(filename: str):
    print("batch.py: Reading data from", filename)
    options = set_options()
    return pd.read_parquet(filename, storage_options=options)


def save_data(df, output_file):
    options = set_options()

    if options:
        print("batch.py: Using to_parquet to write to localstack")
        df.to_parquet(
            output_file,
            engine='pyarrow',
            compression=None,
            index=False,
            storage_options=options
        )
    else:
        print("batch.py: Using awswrangler to write to s3")
        wr.s3.to_parquet(
            df, 
            path=output_file, 
            index=False
            )


def prepare_data(df: pd.DataFrame, categorical: List[str]):
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

    
def main(year: int, month: int):
    # READ MODEL AND DATA
    trip_data_file, test_data_file, predictions_file = path_names(year, month)
    print("batch.py: Trip data:", trip_data_file)
    print("batch.py: Test data:", test_data_file)
    print("batch.py: Predictions data:", predictions_file)

    categorical = ['PULocationID', 'DOLocationID']

    with open('../model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    if os.getenv("TRIP_DATA_FILE_PATTERN") is None:
        print("batch.py: Reading test data file")
        df = read_data(test_data_file)
    else:
        print("batch.py: Reading trip data file")
        df = read_data(trip_data_file)

    # PREP DATA
    df = prepare_data(df, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    # GET BATCH PREDICTIONS ON PREPARED DATA
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('predicted mean duration:', y_pred.mean())

    # SAVE PREDICTIONS
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    print("batch.py: Saving Dataframe with predictions to", predictions_file)
    save_data(df_result, predictions_file)


if __name__ == "__main__":

    year, month = get_year_month()
    print("*"*80)
    print("Starting batch.py for", year, month)

    main(year, month)

