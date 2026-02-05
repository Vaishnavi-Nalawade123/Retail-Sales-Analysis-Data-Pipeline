from pyspark.sql import SparkSession
import os
import subprocess
import glob
import sys

run_date = sys.argv[1] if len(sys.argv) > 1 else None
if not run_date:
    raise ValueError("date_id (YYYYMMDD) required")

spark = SparkSession.builder \
    .appName("Export Gold CSV") \
    .getOrCreate()

gold_tables = {
    "gold_daily_store_revenue": f"/user/sunbeam/data/gold/gold_daily_store_revenue/date_id={run_date}",
    "gold_daily_overall_sales": f"/user/sunbeam/data/gold/gold_daily_overall_sales/date_id={run_date}",
}

base_dir = f"/home/sunbeam/CDAC/BigDataProject/retail_store_analysis/gold_csv/dt={run_date}"
os.makedirs(base_dir, exist_ok=True)

for table, hdfs_path in gold_tables.items():
    tmp_dir = f"{base_dir}/_tmp_{table}"
    final_csv = f"{base_dir}/{table}.csv"

    df = spark.read.parquet(f"hdfs://localhost:9000{hdfs_path}")

    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv(f"file:{tmp_dir}")

    csv_file = glob.glob(f"{tmp_dir}/part-*.csv")[0]
    subprocess.run(f"mv {csv_file} {final_csv}", shell=True, check=True)
    subprocess.run(f"rm -rf {tmp_dir}", shell=True, check=True)

spark.stop()

