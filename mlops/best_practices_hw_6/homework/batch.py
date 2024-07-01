#!/usr/bin/env python
# coding: utf-8

from typing import List
import sys
import os

import pickle
import pandas as pd


def path_names(year: int, month: int):
    input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    
    # this is supposed to be easier -- why?
    # output_file = f'taxi_type=yellow_year={year:04d}_month={month:02d}.parquet'
    
    output_name = f'yellow_tripdata_{year:04d}-{month:02d}.parquet'
    output_dir = './output'
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    output_file = os.path.join(output_dir, output_name)

    return input_file, output_file
    
def read_data(filename: str):
    df = pd.read_parquet(filename)
    return df
    

def prepare_data(df: pd.DataFrame, categorical: List[str]):
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df

    
def main(year: int, month: int):
    input_file, output_file = path_names(year, month)
    categorical = ['PULocationID', 'DOLocationID']

    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)

    df = read_data(input_file, categorical)
    df = prepare_data(df)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)

    print('predicted mean duration:', y_pred.mean())

    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred

    df_result.to_parquet(output_file, engine='pyarrow', index=False)


if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])

    if (month < 1) or (month > 12):
        print(f"Month must be between 1 and 12, received: {month}")
        sys.exit()

    if year < 1972:
        print(f"Year must be 4 digits; received: {year}")
        sys.exit()

    main(year, month)

