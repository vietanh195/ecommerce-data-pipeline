# process_silver.py
import os
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Cấu hình encoding để in được tiếng Việt trên console Windows
if sys.platform.startswith('win'):
    sys.stdout.reconfigure(encoding='utf-8')

def create_spark_session(use_s3=False):
    """
    Khởi tạo Spark Session với cấu hình phù hợp.
    """
    builder = SparkSession.builder \
        .appName("Production Silver Layer ETL") \
        .master("local[*]")
    
    if use_s3:
        # Tự động phát hiện nếu chạy trong Docker container
        in_docker = os.path.exists('/.dockerenv')
        s3_endpoint = "http://minio:9000" if in_docker else "http://localhost:9000"
        print(f"-> Đang kết nối tới S3 (MinIO) tại endpoint: {s3_endpoint}")
        
        builder = builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", "minio") \
            .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def clean_orders(spark, path_orders):
    print("-> Đang xử lý Orders...")
    df_raw = spark.read.csv(path_orders, header=True, inferSchema=True)
    df_clean = df_raw.select(
        col("order_id"),
        col("customer_id"),
        col("order_status").alias("status"),
        col("order_purchase_timestamp").cast("timestamp").alias("purchase_timestamp"),
        col("order_approved_at").cast("timestamp").alias("approved_at"),
        col("order_delivered_carrier_date").cast("timestamp").alias("delivered_carrier_date"),
        col("order_delivered_customer_date").cast("timestamp").alias("delivered_customer_date"),
        col("order_estimated_delivery_date").cast("timestamp").alias("estimated_delivery_date")
    ).dropDuplicates(["order_id"])
    return df_clean

def clean_customers(spark, path_customers):
    print("-> Đang xử lý Customers...")
    df_raw = spark.read.csv(path_customers, header=True, inferSchema=True)
    df_clean = df_raw.select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_code_prefix").cast("int").alias("zip_code_prefix"),
        col("customer_city").alias("city"),
        col("customer_state").alias("state")
    ).dropDuplicates(["customer_id"])
    return df_clean

def clean_order_items(spark, path_items):
    print("-> Đang xử lý Order Items...")
    df_raw = spark.read.csv(path_items, header=True, inferSchema=True)
    df_clean = df_raw.select(
        col("order_id"),
        col("order_item_id").cast("int"),
        col("product_id"),
        col("seller_id"),
        col("shipping_limit_date").cast("timestamp").alias("shipping_limit_date"),
        col("price").cast("double"),
        col("freight_value").cast("double")
    ).dropDuplicates(["order_id", "order_item_id"])
    return df_clean

def ensure_bucket_exists(bucket_name, s3_endpoint):
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client(
        "s3",
        endpoint_url = s3_endpoint,
        aws_access_key_id = "minio",
        aws_secret_access_key = "minio123",
        region_name = "us-east-1",
        use_ssl = False
    )
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError:
        print(f"-> Bucket '{bucket_name}' chưa tồn tại. Đang tự động tạo...")
        s3.create_bucket(Bucket=bucket_name)
        print(f"-> Đã tạo thành công bucket '{bucket_name}'.")

def main():
    parser = argparse.ArgumentParser(description="ETL Bronze to Silver Layer")
    parser.add_argument("--use-s3", action="store_true", help="Sử dụng MinIO S3 làm Storage thay vì Local")
    args = parser.parse_args()
    
    # Thiết lập đường dẫn tương ứng với môi trường
    if args.use_s3:
        print("Đang chạy ETL với lưu trữ S3 (MinIO)...")
        in_docker = os.path.exists('/.dockerenv')
        s3_endpoint = "http://minio:9000" if in_docker else "http://localhost:9000"
        
        # Đảm bảo các bucket cần thiết tồn tại
        ensure_bucket_exists("bronze-layer", s3_endpoint)
        ensure_bucket_exists("silver-layer", s3_endpoint)
        
        path_orders = "s3a://bronze-layer/olist_orders_dataset.csv"
        path_customers = "s3a://bronze-layer/olist_customers_dataset.csv"
        path_items = "s3a://bronze-layer/olist_order_items_dataset.csv"
        
        silver_orders_out = "s3a://silver-layer/orders"
        silver_customers_out = "s3a://silver-layer/customers"
        silver_items_out = "s3a://silver-layer/order_items"
    else:
        print("Đang chạy ETL với lưu trữ Cục bộ (Local)...")
        # Xác định thư mục gốc dự án tương đối với file script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(script_dir))
        
        path_orders = os.path.join(project_root, "data", "raw", "olist_orders_dataset.csv")
        path_customers = os.path.join(project_root, "data", "raw", "olist_customers_dataset.csv")
        path_items = os.path.join(project_root, "data", "raw", "olist_order_items_dataset.csv")
        
        silver_orders_out = os.path.join(project_root, "data", "silver", "orders")
        silver_customers_out = os.path.join(project_root, "data", "silver", "customers")
        silver_items_out = os.path.join(project_root, "data", "silver", "order_items")
        
        # Đảm bảo các thư mục đầu ra tồn tại ở local
        os.makedirs(os.path.join(project_root, "data", "silver"), exist_ok=True)
        
    spark = create_spark_session(use_s3=args.use_s3)
    
    try:
        # 1. Orders
        df_orders = clean_orders(spark, path_orders)
        df_orders.write.mode("overwrite").parquet(silver_orders_out)
        print(f"Ghi thành công {df_orders.count()} dòng Orders.")
        
        # 2. Customers
        df_customers = clean_customers(spark, path_customers)
        df_customers.write.mode("overwrite").parquet(silver_customers_out)
        print(f"Ghi thành công {df_customers.count()} dòng Customers.")
        
        # 3. Order Items
        df_items = clean_order_items(spark, path_items)
        df_items.write.mode("overwrite").parquet(silver_items_out)
        print(f"Ghi thành công {df_items.count()} dòng Order Items.")
        
        print("Hoàn tất pipeline ETL Silver Layer thành công!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
