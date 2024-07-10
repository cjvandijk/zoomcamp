from mlops.best_practices_hw_6.homework.utils.s3_service import S3Storage

if __name__ == "__main__":
    """S3 services"""

    bucket_name = "nyc-duration-claudia"
    year = 2023
    month = 3

    s3 = S3Storage()
    try:
        s3.create_s3_bucket(bucket_name)
    except Exception as e:
        print(e)
    predictions_path = "/Users/cj/Documents/Projects/zoomcamp-homework/mlops/best_practices_hw_6/homework/output/yellow_tripdata_2023-03.parquet"
    s3.upload_to_aws('model/model.pkl', bucket_name, 'model/model.pkl')
    s3.upload_to_aws(predictions_path, bucket_name, f'{year:04d}-{month:02d}/predictions.parquet')

    print(s3.list_buckets())
    print(s3.list_data_in_bucket(bucket_name))