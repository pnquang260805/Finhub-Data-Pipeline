import boto3

from dataclasses import dataclass

@dataclass
class S3Service:
    access_key: str
    secret_key: str
    endpoint: str = "http://minio:9000"
    region: str = "us-east-1"

    def __post_init__(self):
        self.client = boto3.client('s3', aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key, endpoint_url=self.endpoint)

    def write_object(self, bucket : str, path : str, data : object) -> None:
        try:
            self.client.upload_fileobj(Bucket=bucket, Key=path, Fileobj=data)
        except Exception as e:
            print(e)
            raise(e)
    
