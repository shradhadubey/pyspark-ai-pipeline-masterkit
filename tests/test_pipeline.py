import pytest
from pyspark.sql import SparkSession
from src.transform import DataTransformer

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("Testing").getOrCreate()

def test_clean_sales_data(spark):
    transformer = DataTransformer(spark)
    data = [("1", "2026-01-01", " shradha ", -10.0, "IN")]
    schema = ["order_id", "transaction_date", "customer_name", "amount", "country"]
    df = spark.createDataFrame(data, schema)
    
    cleaned_df = transformer.clean_sales_data(df)
    result = cleaned_df.collect()[0]
    
    # Assertions
    assert result["amount"] == 10.0  # Absolute value check
    assert result["customer_name"] == "SHRADHA"  # Trim and Upper check