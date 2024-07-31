# Experiment Tracking with MLFlow

The goal of this project was to use MLflow for experiment tracking and model management. 
1. Datasets are loaded and processed for training
1. A random forest regressor model is trained without hyperparameter tuning, and tracked through MLflow's autolog functionality.
1. A MLflow experiment is tracked while optimizing the random forest regressor with hyperopt, saving the hyperparameters used and the resulting RMSE
1. The best model is selected based on RMSE, and then promoted to MLflow's Model Registry to indicate which model should be deployed.

## Datasets used

This project uses Green Taxi Trip Records datasets to predict the duration of each trip. Data was downloaded for January, February and March 2023 in parquet format from [here](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Preprocessing the data

The script `preprocess_data.py` reads and process the data prior to model training:

* loads the data from the `../../data` folder,
* fits a `DictVectorizer` on the training set (January 2023 data),
* saves the preprocessed datasets and the `DictVectorizer`.

To run, execute this command after installing python 3.12 and requirements.txt:

> ```
> python preprocess_data.py
> ```


## Training a model with autolog

The `train.py` script trains a `RandomForestRegressor` model on the taxi dataset.

The script will:

* load the datasets produced by preprocess_data.py,
* train the model on the training set,
* calculate the RMSE score on the validation set.

In this step, the training hyperparameters are designed to get the training to finish quickly rather than optimizing the model. The script uses MLflow's autologging to track metrics and artifacts of the model training. After executing the script, launch the MLflow UI to see the experiment tracking.

To run:

> ```
> python train.py
> ```
> 
> Start the MLflow Server:
> ```
> mlflow server --backend-store-uri sqlite:///mlflow.db default-artifact-root ./artifacts
> ```
> 
> Then in browser, go to url `http://localhost:5000`

## Tune the model hyperparameters

With the MLflow server still running, an attempt will be made to reduce the validation error thru hyperparameter tuning with `hyperopt`.

The `hpo.py` logs the RMSE (root mean squared error) metric to the tracking server for each optimization run. Autologging is not used in this script. Instead it just logs the list of hyperparameters passed to the `objective` function and the corresponding RMSE of the validation set.

To run:

> Keep the mlflow server running. If it is down, re-execute the command above to start it again.
> 
> ```
> python hpo.py
> ```
> 
> Return to the MLflow UI on your browser and explore the runs from the experiment called `random-forest-hyperopt`.

## Promoting the best model to the MLflow Model Registry

The results from the hyperparameter optimization are good. It's time to test the best model in production. The `register_model.py` will check the results from hyperparameter tuning, select the 5 top runs, and promote the best model (lowest RMSE) to the MLflow Model Registry. 

Finally, the script calculates the RMSE of those models on the test set (March 2023 data) and saves the results to a new experiment called `random-forest-best-models`.

To run:

> Keep the mlflow server running. If it is down, re-execute the command above to start it again.
> 
> ```
> python register_model.py
> ```
> 
> Return to the MLflow UI on your browser and explore the Model Registry tab for the experiment called `random-forest-best-models`.



> Credit:
> The text from this readme is an edited version of a project assignment from Module 3 of [DataTalksClub's](https://datatalks.club) [MLOps Zoomcamp](https://datatalks.club/blog/mlops-zoomcamp.html) course.
