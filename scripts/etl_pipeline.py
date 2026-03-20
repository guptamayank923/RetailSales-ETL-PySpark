import config
from utils import get_spark_session, read_raw_data
from pyspark.sql.functions import col, year, month, sum as _sum

# Start Spark
spark = get_spark_session()

# 1️⃣ Extract
df = read_raw_data(config.RAW_DATA_PATH, spark)

# 2️⃣ Transform
df = df.dropna()

df = df.withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))

df = df.withColumn("Year", year(col("InvoiceDate")))
df = df.withColumn("Month", month(col("InvoiceDate")))

# 3️⃣ Aggregation
monthly_rev = df.groupBy("Year", "Month") \
    .agg(_sum("TotalAmount").alias("MonthlyRevenue"))

top_products = df.groupBy("StockCode") \
    .agg(_sum("TotalAmount").alias("ProductRevenue")) \
    .orderBy(col("ProductRevenue").desc())

# 4️⃣ Load
monthly_rev.write.mode("overwrite") \
    .parquet(config.PROCESSED_PATH + "monthly_revenue/")

top_products.write.mode("overwrite") \
    .parquet(config.PROCESSED_PATH + "top_products/")