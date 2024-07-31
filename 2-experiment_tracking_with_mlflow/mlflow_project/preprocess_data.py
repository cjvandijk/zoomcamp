import os
import pickle
import pandas as pd

from sklearn.feature_extraction import DictVectorizer

"""
Read data downloaded from New York Taxi Trip Duration datasets.

This is run manually from command line.

Classes:

    Pickler
    Unpickler

Functions:

    dump(object, file)
    dumps(object) -> string
    load(file) -> object
    loads(string) -> object

Misc variables:

    __version__
    format_version
    compatible_formats

"""
RAW_DATA_PATH = "../../data"
PROCESSED_DATA_PATH = "training_data"

def dump_pickle(obj, filename: str):
    """
    Save object as pickle file to destination supplied.
    
    Args:
    obj: a DictVectorizer or Models"""
    with open(filename, "wb") as f_out:
        return pickle.dump(obj, f_out)


def read_dataframe(filename: str):
    df = pd.read_parquet(filename)

    df['duration'] = df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']
    df.duration = df.duration.apply(lambda td: td.total_seconds() / 60)
    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)

    return df


def preprocess(df: pd.DataFrame, dv: DictVectorizer, fit_dv: bool = False):
    df['PU_DO'] = df['PULocationID'] + '_' + df['DOLocationID']
    categorical = ['PU_DO']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')
    if fit_dv:
        X = dv.fit_transform(dicts)
    else:
        X = dv.transform(dicts)
    return X, dv


def run_data_prep(dataset: str = "green"):
    """
    Calls read_data_frame() function to read data, extracts target,
    pre-processes data and fits a DictVectorizer, saves processed 
    data and DictVectorizer.

    Args:
        dataset: "yellow" or "green" taxi data

    Returns:
        No return value. Data is saved to path specified by the
        PROCESSED_DATA_PATH variable.
    """

    # Load parquet files
    df_train = read_dataframe(
        os.path.join(RAW_DATA_PATH, f"{dataset}_tripdata_2023-01.parquet")
    )
    df_val = read_dataframe(
        os.path.join(RAW_DATA_PATH, f"{dataset}_tripdata_2023-02.parquet")
    )
    df_test = read_dataframe(
        os.path.join(RAW_DATA_PATH, f"{dataset}_tripdata_2023-03.parquet")
    )

    # Extract the target
    target = 'duration'
    y_train = df_train[target].values
    y_val = df_val[target].values
    y_test = df_test[target].values

    # Fit the DictVectorizer and preprocess data
    dv = DictVectorizer()
    X_train, dv = preprocess(df_train, dv, fit_dv=True)
    X_val, _ = preprocess(df_val, dv, fit_dv=False)
    X_test, _ = preprocess(df_test, dv, fit_dv=False)

    # Create dest_path folder unless it already exists
    os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

    # Save DictVectorizer and datasets
    dump_pickle(dv, os.path.join(PROCESSED_DATA_PATH, "dv.pkl"))
    dump_pickle((X_train, y_train), os.path.join(PROCESSED_DATA_PATH, "train.pkl"))
    dump_pickle((X_val, y_val), os.path.join(PROCESSED_DATA_PATH, "val.pkl"))
    dump_pickle((X_test, y_test), os.path.join(PROCESSED_DATA_PATH, "test.pkl"))


if __name__ == '__main__':
    run_data_prep()
