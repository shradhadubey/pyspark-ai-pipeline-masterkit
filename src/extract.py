import pandas as pd
from pyspark.sql import SparkSession, DataFrame

def extract_from_s3(spark: SparkSession, s3_path: str) -> DataFrame:
    """
    Reads raw CSV data from an S3 path.
    """
    try:
        return spark.read.option("header", "true").csv(s3_path)
    except Exception as e:
        print(f"Error extracting from S3: {e}")
        # Return empty DF on failure for pipeline stability
        return spark.createDataFrame([], [])

def fetch_mock_api_data() -> pd.DataFrame:
    """
    Simulates fetching data from a REST API.
    """
    data = {
        "order_id": ["101", "102", "103"],
        "transaction_date": ["2026-02-01", "2026-02-02", "2026-02-03"],
        "customer_name": ["Alice", "Bob", None],
        "amount": [250.00, -50.0, 300.25],
        "country": ["US", "UK", "IN"]
    }
    return pd.DataFrame(data)