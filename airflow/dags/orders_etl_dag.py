from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_SUBMIT_COMMAND = """
docker exec orders_spark /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3 \
  /app/spark/jobs/orders_etl.py
"""


with DAG(
    dag_id="orders_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["orders", "spark", "etl"],
) as dag:
    run_spark_orders_etl = BashOperator(
        task_id="run_spark_orders_etl",
        bash_command=SPARK_SUBMIT_COMMAND,
    )