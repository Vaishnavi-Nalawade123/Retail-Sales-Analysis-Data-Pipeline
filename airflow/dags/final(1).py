from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "sunbeam",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_daily_pipeline_final_version",
    default_args=default_args,
    description="Daily Retail Pipeline with Gold CSV Snapshots",
    schedule_interval="50 11 * * *",     # Runs every day at 11:50
    start_date=datetime(2026, 1, 25),    # Backfill from Jan 25, 2026
    catchup=True,                        # ENABLED for historical runs
    tags=["retail", "daily", "gold"],
) as dag:

    # --------------------------------------------------
    # START
    # --------------------------------------------------
    start = BashOperator(
        task_id="start",
        bash_command="echo 'DAILY RETAIL PIPELINE STARTED FOR {{ ds }}'"
    )

    # --------------------------------------------------
    # COPY RAW ORDERS → HDFS (BRONZE)
    # --------------------------------------------------
    copy_to_hdfs = BashOperator(
        task_id="copy_orders_to_hdfs",
        bash_command="""
        hdfs dfs -mkdir -p /user/sunbeam/data/bronze/orders/{{ ds }} &&
        hdfs dfs -put -f \
        /home/sunbeam/CDAC/BigDataProject/retail_store_analysis/orders/orders_{{ ds_nodash }}.json \
        /user/sunbeam/data/bronze/orders/{{ ds }}/
        """
    )

    # --------------------------------------------------
    # SPARK SILVER LAYER
    # --------------------------------------------------
    spark_silver = BashOperator(
        task_id="spark_silver",
        bash_command="""
        spark-submit \
        /home/sunbeam/CDAC/BigDataProject/retail_store_analysis/spark/spark_silver_fact_orders.py \
        {{ ds_nodash }}
        """
    )

    # --------------------------------------------------
    # SPARK GOLD LAYER  FIXED
    # --------------------------------------------------
    spark_gold = BashOperator(
        task_id="spark_gold",
        bash_command="""
        spark-submit \
        /home/sunbeam/CDAC/BigDataProject/retail_store_analysis/spark/spark_gold_tables.py \
        {{ ds_nodash }}
        """
    )

    # --------------------------------------------------
    # EXPORT GOLD → DATE-WISE CSV SNAPSHOTS
    # --------------------------------------------------
    export_gold_csv = BashOperator(
        task_id="export_gold_to_csv",
        bash_command="""
        spark-submit \
        /home/sunbeam/CDAC/BigDataProject/retail_store_analysis/spark/export_all_gold_to_csv.py \
        {{ ds_nodash }}
        """
    )

    # --------------------------------------------------
    # OPTIONAL EMAIL / NOTIFICATION
    # --------------------------------------------------
    optional_email = BashOperator(
        task_id="optional_email",
        bash_command="""
        echo "Email skipped for {{ ds }} (optional notification)."
        """,
        trigger_rule="all_done"
    )

    # --------------------------------------------------
    # END
    # --------------------------------------------------
    end = BashOperator(
        task_id="end",
        bash_command="echo 'PIPELINE COMPLETED FOR {{ ds }}'"
    )

    # DAG FLOW
    start >> copy_to_hdfs >> spark_silver >> spark_gold >> export_gold_csv >> optional_email >> end
