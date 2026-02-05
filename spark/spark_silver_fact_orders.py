import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_timestamp, to_date, date_format, expr


if len(sys.argv) != 2:
    raise ValueError("date_id (YYYYMMDD) must be provided")

date_id = sys.argv[1]                 # 20260126
ds = f"{date_id[:4]}-{date_id[4:6]}-{date_id[6:]}"  # 2026-01-26


spark = SparkSession.builder \
    .appName("silver_fact_orders") \
    .getOrCreate()


bronze_path = (
    f"hdfs:///user/sunbeam/data/bronze/orders/{ds}/"
    f"orders_{date_id}.json"
)

orders_df = spark.read \
    .option("multiline", "true") \
    .json(bronze_path)


fact_df = (
    orders_df
    .withColumn("item", explode(col("items")))
    .select(
        "order_id",
        "store_id",
        col("item.product_id").alias("product_id"),
        col("item.quantity").alias("quantity"),
        col("item.unit_price").alias("unit_price"),
        "order_timestamp"
    )
)


fact_df = (
    fact_df
    .withColumn("order_ts", to_timestamp("order_timestamp"))
    .withColumn("order_date", to_date("order_ts"))
    .withColumn("date_id", date_format("order_date", "yyyyMMdd").cast("int"))
)


fact_df = (
    fact_df
    .filter(col("quantity") > 0)
    .filter(col("unit_price") > 0)
    .withColumn("order_amount", expr("quantity * unit_price"))
    .select(
        "order_id",
        "store_id",
        "product_id",
        "date_id",
        "quantity",
        "order_amount"
    )
)


fact_df.write \
    .mode("append") \
    .partitionBy("date_id") \
    .parquet("hdfs:///user/sunbeam/data/silver/fact_orders")

spark.stop()

