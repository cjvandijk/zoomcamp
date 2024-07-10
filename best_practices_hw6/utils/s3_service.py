import boto3

class S3Storage:
    """S3 storage API Implementations"""

    def __init__(self) -> None:
        # super().__init__()
        self._endpoint_url = "http://localhost:4566"
        self._s3_resource = boto3.resource(
            's3',
            region_name='us-east-2',
            endpoint_url=self._endpoint_url
            )
        self._s3_client = boto3.client(
            's3', 
            region_name='us-west-2', 
            endpoint_url=self._endpoint_url
            )

    def create_s3_bucket(self, bucket_name):
        """ creates s3 bucket """
        self._s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'us-east-2'}
            )

    def upload_to_aws(self, local_file, bucket_name, s3_file):
        """Uploads file to s3 bucket"""
        self._s3_client.upload_file(local_file, bucket_name, s3_file)

    def list_buckets(self):
        """List all buckets of a S3 region"""
        list_bucket = []
        for bucket in self._s3_resource.buckets.all():
            list_bucket.append(bucket.name )
        return list_bucket

    def list_data_in_bucket(self, bucket_name) :
        """Lists all data in a specified s3 bucket of a region"""
        list_data = []
        bucket = self._s3_resource.Bucket(bucket_name)
        for bucket_object in bucket.objects.all():
            list_data.append(bucket_object.key)
        return list_data