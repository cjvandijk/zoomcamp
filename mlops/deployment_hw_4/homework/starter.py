#!/usr/bin/env python
# coding: utf-8

import argparse
# import logging

# import boto3
# from botocore.exceptions import ClientError
# import s3fs
import pickle
import pandas as pd
from typing import Tuple


def parse_arguments() -> Tuple[str, str]: 
    parser = argparse.ArgumentParser(description='Enter desired year (YYYY) and month (MM) of NY taxi data')
    parser.add_argument('--year', action="store", dest='year', default='2023')
    parser.add_argument('--month', action="store", dest='month', default='03')
    args = parser.parse_args()
    return args.year, args.month


def read_data(filename: str) -> pd.DataFrame:
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


def dataframe_to_s3(s3_client, input_datafame, bucket_name, filepath, format):
        if format == 'parquet':
            out_buffer = BytesIO()
            input_datafame.to_parquet(out_buffer, index=False)

        elif format == 'csv':
            out_buffer = StringIO()
            input_datafame.to_parquet(out_buffer, index=False)

        s3_client.put_object(Bucket=bucket_name, Key=filepath, Body=out_buffer.getvalue())


OUTPUT_FILE = "ride_pred_results.parquet"
categorical = ['PULocationID', 'DOLocationID']
year, month = parse_arguments()

print("Loading model.bin")
with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)

print(f"Loading yellow_tripdata_{year}-{month}.parquet ")
df = read_data(f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet')

print("Predicting durations for the dataset\n")
dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)


print("Mean predicted durations:", y_pred.mean())

df['ride_id'] = f'{int(year):04d}/{int(month):02d}_' + df.index.astype('str')
df['predicted_duration'] = y_pred

df_result = df[['ride_id', 'predicted_duration']]

print(f"Saving results to {OUTPUT_FILE}")
df_result.to_parquet(
    OUTPUT_FILE,
    engine='pyarrow',
    compression=None,
    index=False
)

print(f"Uploading {OUTPUT_FILE} to s3://claudia-mlops.")
df.to_parquet(f's3://claudia-mlops/results/{OUTPUT_FILE}.gzip',
              engine='pyarrow',
              compression=None,
              index=False)



# s3 = s3fs.S3FileSystem(anon=False)  # uses default credentials
# with s3.open(f'claudia-mlops/results/{OUTPUT_FILE}', 'wb') as f:
#     df_result.to_parquet(
#         OUTPUT_FILE,
#         engine='pyarrow',
#         compression=None,
#         index=False
#     )
# s3.du('mybucket/new-file')