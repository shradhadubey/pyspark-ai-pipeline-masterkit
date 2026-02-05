from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataTransformer:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def clean_sales_data(self, df: DataFrame) -> DataFrame:
        """
        Cleans raw sales data: handles nulls, formats dates, and calculates totals.
        """
        logger.info("Starting transformation: Cleaning Sales Data")
        
        return df.select(
            F.col("order_id").cast("string"),
            F.to_date(F.col("transaction_date"), "yyyy-MM-dd").alias("date"),
            F.trim(F.upper(F.col("customer_name"))).alias("customer_name"),
            F.abs(F.col("amount")).alias("amount"),  # Ensure no negative sales
            F.when(F.col("country").isNull(), "Unknown").otherwise(F.col("country")).alias("country")
        ).filter(F.col("amount") > 0)

    def enrich_with_ai_logic(self, df: DataFrame) -> DataFrame:
        """
        Example of how to structure an AI-enrichment 'placeholder' 
        for LLM sentiment analysis on customer reviews.
        """
        # In a real pipeline, you'd use a Pandas UDF for high-performance AI inference
        return df.withColumn(
            "data_quality_score", 
            F.when(F.col("customer_name").isNotNull(), 1.0).otherwise(0.5)
        )

if __name__ == "__main__":
    # Local Test Run
    spark = SparkSession.builder.appName("MasterkitTransformation").getOrCreate()
    
    # Mock Data for demonstration
    data = [("1", "2026-02-01", "  shradha dubey  ", 150.50, "IN"),
            ("2", "2026-02-02", None, -20.0, "US")]
    schema = ["order_id", "transaction_date", "customer_name", "amount", "country"]
    
    raw_df = spark.createDataFrame(data, schema)
    
    transformer = DataTransformer(spark)
    clean_df = transformer.clean_sales_data(raw_df)
    enriched_df = transformer.enrich_with_ai_logic(clean_df)
    
    enriched_df.show()