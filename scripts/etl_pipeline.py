# import config
# from utils import get_spark_session, read_raw_data
# from pyspark.sql.functions import col, year, month, sum as _sum

# # Start Spark
# spark = get_spark_session()

# # 1️⃣ Extract
# df = read_raw_data(config.RAW_DATA_PATH, spark)

# # 2️⃣ Transform
# df = df.dropna()

# df = df.withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))

# df = df.withColumn("Year", year(col("InvoiceDate")))
# df = df.withColumn("Month", month(col("InvoiceDate")))

# # 3️⃣ Aggregation
# monthly_rev = df.groupBy("Year", "Month") \
#     .agg(_sum("TotalAmount").alias("MonthlyRevenue"))

# top_products = df.groupBy("StockCode") \
#     .agg(_sum("TotalAmount").alias("ProductRevenue")) \
#     .orderBy(col("ProductRevenue").desc())

# # 4️⃣ Load
# monthly_rev.write.mode("overwrite") \
#     .parquet(config.PROCESSED_PATH + "monthly_revenue/")

# top_products.write.mode("overwrite") \
#     .parquet(config.PROCESSED_PATH + "top_products/")

import config
from utils import get_spark_session, read_raw_data
from pyspark.sql.functions import col, year, month, sum as _sum
import logging

# ---------------- LOGGING CONFIG ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------- MAIN PIPELINE ----------------
def main():
    try:
        logging.info("Starting ETL Pipeline")

        # Start Spark
        spark = get_spark_session()

        # ---------------- EXTRACT ----------------
        logging.info("Reading raw data...")
        df = read_raw_data(config.RAW_DATA_PATH, spark)

        logging.info(f"Total records after ingestion: {df.count()}")

        # ---------------- VALIDATION ----------------
        logging.info("Validating data...")

        required_columns = ["InvoiceDate", "Quantity", "UnitPrice", "StockCode"]

        for col_name in required_columns:
            if col_name not in df.columns:
                raise ValueError(f"Missing required column: {col_name}")

        null_count = df.filter(
            col("InvoiceDate").isNull() |
            col("Quantity").isNull() |
            col("UnitPrice").isNull()
        ).count()

        logging.info(f"Null records found: {null_count}")
        logging.info("Validation completed")

        # ---------------- TRANSFORM ----------------
        logging.info("Applying transformations...")

        df = df.dropna()

        df = df.withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))
        df = df.withColumn("Year", year(col("InvoiceDate")))
        df = df.withColumn("Month", month(col("InvoiceDate")))

        logging.info("Transformation completed")

        # ---------------- AGGREGATION ----------------
        logging.info("Performing aggregations...")

        monthly_rev = df.groupBy("Year", "Month") \
            .agg(_sum("TotalAmount").alias("MonthlyRevenue"))

        top_products = df.groupBy("StockCode") \
            .agg(_sum("TotalAmount").alias("ProductRevenue")) \
            .orderBy(col("ProductRevenue").desc())

        logging.info("Aggregation completed")

        # ---------------- LOAD ----------------
        logging.info("Writing output to Parquet...")

        monthly_rev.write.mode("overwrite") \
            .parquet(config.PROCESSED_PATH + "monthly_revenue/")

        top_products.write.mode("overwrite") \
            .parquet(config.PROCESSED_PATH + "top_products/")

        logging.info("ETL Pipeline completed successfully")

    except Exception as e:
        logging.error(f"Pipeline failed due to error: {str(e)}")
        raise


# ---------------- ENTRY POINT ----------------
if __name__ == "__main__":
    main()