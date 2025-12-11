from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "arda",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ride_sharing_batch_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=1),   # Airflow 3 syntax
    catchup=False,
    default_args=default_args,
    tags=["ride-sharing", "etl"],
):

    silver_task = BashOperator(
        task_id="run_silver_job",
        bash_command="python3 spark_jobs/silver/silver_batch_transform.py",
    )

    gold_hourly_task = BashOperator(
        task_id="run_gold_hourly_revenue",
        bash_command="python3 spark_jobs/gold/gold_hourly_revenue.py",
    )

    gold_driver_stats_task = BashOperator(
        task_id="run_gold_driver_stats",
        bash_command="python3 spark_jobs/gold/gold_driver_stats.py",
    )

    gold_status_task = BashOperator(
        task_id="run_gold_daily_status",
        bash_command="python3 spark_jobs/gold/gold_status_daily.py",
    )

    # Dependencies
    silver_task >> gold_hourly_task
    silver_task >> gold_driver_stats_task
    silver_task >> gold_status_task
