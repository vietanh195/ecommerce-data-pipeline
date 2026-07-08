# upload_bronze

import boto3
from pathlib import Path
from botocore.exceptions import ClientError

s3 = boto3.client(
    "s3",
    endpoint_url = "http://localhost:9000",
    aws_access_key_id = "minio",
    aws_secret_access_key = "minio123",
    region_name = "us-east-1",
    use_ssl = False
)

try:
    s3.head_bucket(Bucket="bronze-layer")
    print("Bucket 'bronze_layer' already exists.")
except ClientError as e:
    print(e.response['Error']['Code'])
    print("Bucket 'bronze_layer' created.")
    s3.create_bucket(Bucket="bronze-layer")

folder = Path("data/raw")
files = folder.glob("*.csv")
for file in files:
    s3.upload_file(
        Filename = str(file),
        Bucket = "bronze-layer",
        Key = file.name
    )

    print(f"{file.name} uploaded to S3 bucket 'bronze-layer'...")
