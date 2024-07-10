## Code snippets

### Building and running Docker images

```bash
docker build -t $(LOCAL_IMAGE_NAME) .
```

```bash
docker run -it --rm \
    -v ~/.aws:/root/.aws \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e RUN_ID="da8a85f0cfc94022891e42f77ed37298" \
    -e TEST_RUN="True" \
    -e AWS_DEFAULT_REGION="us-east-1" \
    $(LOCAL_IMAGE_NAME)
```

Start localstack service in homework directory
`docker compose up`
Localstack:s3 will be available on port 4566. Note that stopping the container with `docker-compose down` will remove ALL DATA as well.

To list all buckets on localstack:
`aws s3api list-buckets --endpoint-url=http://localhost:4566`

To show contents of specific localstack:s3 location:
`aws s3 ls --endpoint-url=http://localhost:4566 s3://nyc-duration-claudia/`

To make bucket:
`aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration-claudia`

Run test that creates a new bucket on localstack and copies local model into it:
`python test_s3.py`

export PREDICTIONS_FILE_PATTERN="s3://{bucket}/{year:04d}-{month:02d}/predictions.parquet"
export TRIP_DATA_FILE_PATTERN="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
export TEST_DATA_FILE_PATTERN="s3://{bucket}/{year:04d}-{month:02d}/test_data.parquet"
export BUCKET="nyc-duration-claudia"
export S3_ENDPOINT_URL="http://localhost:4566"

Run batch.py with YYYY and MM args
`python batch.py 2023 3`

## ISSUES
HW Q4
* Reading from Localstack s3 with Pandas says: "So far we've been reading parquet files from S3 with using pandas read_parquet" but this is not true. We have been reading from the trip-data on the web.
* It also says "In our script, we write data to S3." But in Q1 we were instructed to write to local drive: "To make it easier to run it, you can write results to your local filesystem. E.g. here: output_file = f'taxi_type=yellow_year={year:04d}_month={month:02d}.parquet'"
* At the start of Question 4, make it clearer that we are now talking about editing batch.py, not test_batch.py. Because in Question 3 we were editing test_batch.py. And Q4 doesn't indicate we've moved back to batch.py.
* Is this really what you want in the pattern? "taxi_type=fhv/year={year:04d}/month={month:02d}/predictions.parquet" -- it makes for a strange output filename: "s3://nyc-duration-claudia/taxi_type=fhv/year=2022/month=03/predictions.parquet"
* These in/out file paths are a bit confusing. When I run the batch it can put the output in the /out folder, but when I read it in the test script, it will not read it from /in, because it's not there. Why not eliminate the in/out bit and just have file paths? (export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet", export OUTPUT_FILE_PATTERN="s3://nyc-duration/out/...)
* When making input and output paths configurable, the input path reads data of a completely different format depending on whether you use the default_input_pattern (which reads nyc taxi data from the trip data site) or INPUT_FILE_PATTERN, which reads the processed data from s3 containing only the ride ID and prediction columns. Reading the latter causes further processing of the data to fail.
