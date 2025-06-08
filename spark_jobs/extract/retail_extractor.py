# spark_jobs/extract/retail_extractor.py
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def extract_retail_data():
    spark = SparkSession.builder \
        .appName("OnlineRetailExtractor") \
        .getOrCreate()

    # Define schema for online retail data
    retail_schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", TimestampType(), True),
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True)
    ])

    # Read CSV file (replace with your actual data path)
    raw_df = spark.read \
        .schema(retail_schema) \
        .option("header", "true") \
        .csv("/data/raw/online_retail.csv")

    # Write as Parquet for efficient processing
    raw_df.write \
        .mode("overwrite") \
        .parquet("/data/processed/raw_retail_data.parquet")

    spark.stop()

if __name__ == "__main__":
    extract_retail_data()