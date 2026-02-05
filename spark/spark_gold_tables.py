import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as _sum, col, rank, lit


if len(sys.argv) != 2:
    raise ValueError("date_id (YYYYMMDD) must be provided")

date_id = int(sys.argv[1])


spark = SparkSession.builder \
    .appName("gold_layer_business_analytics") \
    .getOrCreate()


fact_orders = spark.read.parquet(
    f"hdfs:///user/sunbeam/data/silver/fact_orders/date_id={date_id}"
)


fact_orders = fact_orders.withColumn("date_id", lit(date_id))


dim_product = spark.read.parquet(
    "hdfs:///user/sunbeam/data/silver/dim_product"
)

dim_store = spark.read.parquet(
    "hdfs:///user/sunbeam/data/silver/dim_store"
)

dim_date = spark.read.parquet(
    "hdfs:///user/sunbeam/data/silver/dim_date"
)


gold_daily_store_revenue = (
    fact_orders
    .groupBy("date_id", "store_id")
    .agg(
        _sum("order_amount").alias("daily_store_revenue")
    )
)

gold_daily_store_revenue.write \
    .mode("append") \
    .partitionBy("date_id") \
    .parquet("hdfs:///user/sunbeam/data/gold/gold_daily_store_revenue")


fact_product = fact_orders.join(dim_product, "product_id", "left")

daily_product_sales = (
    fact_product
    .groupBy("date_id", "store_id", "product_id", "product_name")
    .agg(
        _sum("order_amount").alias("total_sales")
    )
)

rank_window = Window.partitionBy(
    "date_id", "store_id"
).orderBy(
    col("total_sales").desc()
)

gold_top_products = (
    daily_product_sales
    .withColumn("rank", rank().over(rank_window))
    .filter(col("rank") <= 5)
)

gold_top_products.write \
    .mode("append") \
    .partitionBy("date_id") \
    .parquet("hdfs:///user/sunbeam/data/gold/gold_daily_top_products")


fact_category = (
    fact_orders
    .join(dim_product, "product_id", "left")
    .join(dim_store, "store_id", "left")
)

gold_daily_category_sales = (
    fact_category
    .groupBy("date_id", "store_id", "city", "category_id")
    .agg(
        _sum("order_amount").alias("daily_category_sales")
    )
)

gold_daily_category_sales.write \
    .mode("append") \
    .partitionBy("date_id") \
    .parquet("hdfs:///user/sunbeam/data/gold/gold_daily_category_sales")


gold_daily_overall_sales = (
    fact_orders
    .groupBy("date_id")
    .agg(
        _sum("order_amount").alias("total_sales"),
        _sum("quantity").alias("total_quantity")
    )
)

gold_daily_overall_sales.write \
    .mode("append") \
    .partitionBy("date_id") \
    .parquet("hdfs:///user/sunbeam/data/gold/gold_daily_overall_sales")


fact_with_date = fact_orders.join(dim_date, "date_id", "left")

gold_weekly_sales = (
    fact_with_date
    .groupBy("year", "week", "is_weekend")
    .agg(
        _sum("order_amount").alias("total_sales"),
        _sum("quantity").alias("total_quantity")
    )
)

gold_weekly_sales.write \
    .mode("append") \
    .parquet("hdfs:///user/sunbeam/data/gold/gold_weekly_sales")


spark.stop()

