#!/usr/bin/env python
# coding: utf-8

import argparse

import pickle
import pandas as pd
from typing import Tuple

OUTPUT_FILENAME = "ride_pred_results.parquet"

def parse_arguments() -> Tuple[str, str]:

    parser = argparse.ArgumentParser(description='Enter desired year (YYYY) and month (MM) of NY taxi data')
    parser.add_argument('--year', action="store", dest='year', default='2023')
    parser.add_argument('--month', action="store", dest='month', default='03')
    args = parser.parse_args()

    return args.year, args.month


def read_and_process_data(filename: str) -> pd.DataFrame:

    print(f"Loading yellow_tripdata_{year}-{month}.parquet ")

    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


def save_prediction_results(df: pd.DataFrame, y_pred: pd.Series):

    df['ride_id'] = f'{int(year):04d}/{int(month):02d}_' + df.index.astype('str')
    df['predicted_duration'] = y_pred

    df_result = df[['ride_id', 'predicted_duration']].copy()

    print(f"Saving results to {OUTPUT_FILENAME}")
    df_result.to_parquet(
        OUTPUT_FILENAME,
        engine='pyarrow',
        compression=None,
        index=False
    )

    print(f"Uploading {OUTPUT_FILENAME} to s3://claudia-mlops.")
    df.to_parquet(f's3://claudia-mlops/results/{OUTPUT_FILENAME}.gzip',
                engine='pyarrow',
                compression=None,
                index=False)



print("Loading model.bin")
with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)

year, month = parse_arguments()
categorical = ['PULocationID', 'DOLocationID']
df = read_and_process_data(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet')

print(f"Batch predicting durations for the {month}/{year} dataset\n   . . .")
dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)

# answer question 1
print("Mean predicted durations:", y_pred.mean())

save_prediction_results(df, y_pred)

def save_prediction_results(df: pd.DataFrame, y_pred: pd.Series):
    df['ride_id'] = f'{int(year):04d}/{int(month):02d}_' + df.index.astype('str')
    df['predicted_duration'] = y_pred

    df_result = df[['ride_id', 'predicted_duration']]

    print(f"Saving results to {OUTPUT_FILENAME}")
    df_result.to_parquet(
        OUTPUT_FILENAME,
        engine='pyarrow',
        compression=None,
        index=False
    )

    print(f"Uploading {OUTPUT_FILENAME} to s3://claudia-mlops.")
    df.to_parquet(f's3://claudia-mlops/results/{OUTPUT_FILENAME}.gzip',
                engine='pyarrow',
                compression=None,
                index=False)

