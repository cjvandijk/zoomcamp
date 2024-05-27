import os
import pickle
import click
import mlflow
from mlflow.tracking import MlflowClient

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

"""
Trains RandomForestRegressor on data from NYC Green Taxi Trip Records, obtained from
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Launch mlflow ui locally with this command before running this script:
$ mlflow ui --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./artifacts

In this script, the model created in experiment ID="1" will be registered. 
View results in mlflow UI by pointing browser to the "listening at" url, 
e.g. http://127.0.0.1:5000
"""

mlflow.set_tracking_uri("sqlite:///mlflow.db")
mlflow.set_experiment("nyc-taxi-experiment")
LOCAL_TRACKING_SERVER = "http://127.0.0.1:5000"


def load_pickle(filename: str):
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)


@click.command()
@click.option(
    "--data_path",
    default="./output",
    help="Location where the processed NYC taxi trip data was saved"
)
def run_train(data_path: str):
    
    X_train, y_train = load_pickle(os.path.join(data_path, "train.pkl"))
    X_val, y_val = load_pickle(os.path.join(data_path, "val.pkl"))

    mlflow.sklearn.autolog()

    with mlflow.start_run():

        rf = RandomForestRegressor(max_depth=10, random_state=0)
        rf.fit(X_train, y_train)
        y_pred = rf.predict(X_val)

        rmse = mean_squared_error(y_val, y_pred, squared=False)


def register_model():
    client = MlflowClient(LOCAL_TRACKING_SERVER)

    run_id = client.search_runs(experiment_ids='1')[0].info.run_id

    mlflow.register_model(
        model_uri=f"runs:/{run_id}/models",
        name='nyc-taxi-model'
    )


if __name__ == '__main__':
    run_train()
    register_model()


