from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("string")
def detect_sentiment_udf(reviews: pd.Series) -> pd.Series:
    """
    Placeholder for an LLM call (e.g., OpenAI API).
    In a real app, you'd initialize the client here.
    """
    # Logic: If review contains 'good' -> Positive, else Neutral
    return reviews.apply(lambda x: "Positive" if "good" in str(x).lower() else "Neutral")

def enrich_data_with_sentiment(df, column_name):
    return df.withColumn("sentiment", detect_sentiment_udf(df[column_name]))