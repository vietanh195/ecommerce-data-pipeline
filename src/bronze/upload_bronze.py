# upload_bronze

import os
import boto3
from pathlib import Path
from botocore.exceptions import ClientError

# Tự động phát hiện nếu chạy trong Docker container
in_docker = os.path.exists('/.dockerenv')
s3_endpoint = "http://minio:9000" if in_docker else "http://localhost:9000"

s3 = boto3.client(
    "s3",
    endpoint_url = s3_endpoint,
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

# Xác định đường dẫn thư mục data/raw tương đối với file script
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent.parent
folder = project_root / "data" / "raw"
files = list(folder.glob("*.csv"))

if not files:
    print(f"Không tìm thấy file CSV nào tại: {folder}")

for file in files:
    s3.upload_file(
        Filename = str(file),
        Bucket = "bronze-layer",
        Key = file.name
    )

    print(f"{file.name} uploaded to S3 bucket 'bronze-layer'...")
