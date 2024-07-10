#!/usr/bin/env bash

cd "$(dirname "$0")"

docker compose up -d

sleep 1

export PREDICTIONS_FILE_PATTERN="s3://{bucket}/{year:04d}-{month:02d}/predictions.parquet"
export TEST_DATA_FILE_PATTERN="s3://{bucket}/{year:04d}-{month:02d}/test_data.parquet"
export BUCKET="nyc-duration-claudia"
export S3_ENDPOINT_URL="http://localhost:4566"

aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration-claudia

pipenv run python integration_test.py

ERROR_CODE=$?

if [ ${ERROR_CODE} != 0 ]; then
    docker-compose logs
fi

# docker-compose down

# exit ${ERROR_CODE}
