# spark_jobs/load/retail_loader.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def load_to_database():
    spark = SparkSession.builder \
        .appName("OnlineRetailLoader") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

    # Read transformed data
    transformed_df = spark.read.parquet("/data/processed/transformed_retail.parquet")
    
    # Database connection properties
    db_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://postgres:5432/airflow"
    }
    
    # Write to dimension tables
    transformed_df.select(
        col("CustomerID").alias("customer_id"),
        col("Country").alias("country")
    ).distinct() \
     .write \
     .mode("append") \
     .jdbc(db_properties["url"], "dim_customers", properties=db_properties)
    
    # Write to fact table
    transformed_df.select(
        col("InvoiceNo").alias("invoice_no"),
        col("CustomerID").alias("customer_id"),
        col("StockCode").alias("product_id"),
        to_date(col("InvoiceDate")).alias("date_id"),
        col("Quantity").alias("quantity"),
        col("UnitPrice").alias("unit_price"),
        (col("Quantity") * col("UnitPrice")).alias("total_price"),
        col("Country").alias("country")
    ).write \
     .mode("append") \
     .jdbc(db_properties["url"], "fact_sales", properties=db_properties)
    
    spark.stop()

if __name__ == "__main__":
    load_to_database()