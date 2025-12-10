from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Adjust this to your real project path if needed:
PROJECT_ROOT = "/Users/arda/Desktop/ride-sharing-pipeline"

default_args = {
    "owner": "arda",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="ride_sharing_batch_pipeline",
    default_args=default_args,
    description="Run Silver and Gold jobs for the ride-sharing data pipeline",
    schedule_interval="*/10 * * * *",  # every 10 minutes
    start_date=datetime(2025, 12, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1) Silver transform (Bronze -> Silver)
    silver_task = BashOperator(
        task_id="silver_transform",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python3 spark_jobs/silver_transform.py"
        ),
    )

    # 2) Gold: hourly revenue
    gold_hourly_task = BashOperator(
        task_id="gold_hourly_revenue",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python3 spark_jobs/gold_hourly_revenue.py"
        ),
    )

    # 3) Gold: driver stats
    gold_driver_task = BashOperator(
        task_id="gold_driver_stats",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python3 spark_jobs/gold_driver_stats.py"
        ),
    )

    # 4) Gold: ride status daily
    gold_status_task = BashOperator(
        task_id="gold_ride_status_daily",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python3 spark_jobs/gold_ride_status_daily.py"
        ),
    )

    # DAG dependencies:
    # First run Silver, then run all Gold in parallel
    silver_task >> [gold_hourly_task, gold_driver_task, gold_status_task]
