import time
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from airflow.operators.dummy import DummyOperator
import os
from airflow.api.common.experimental import get_task_instance
from airflow.models.dagrun import DagRun

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/mineral-order-337219-e4d095ffa62f.json"  # do not use

# global variables
start_date = datetime(2022, 2, 25)
BUCKET_NAME = "airflow-test-bucket-1107"
DESTINATION_BUCKET_NAME = "airflow-test-bucket-1107"
storage_client = storage.Client()
today_dt = datetime.now().date()
manifest_history_file_list = []

load_ts: datetime

start_date = datetime(2022, 2, 25)
default_args = {
    "owner": "dhruv-airflow-test-dev",
    "depends_on_past": False,
    "start_date": start_date,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("prod-dag-v2", default_args=default_args,
          schedule_interval='13 18 * * *',
          dagrun_timeout=timedelta(seconds=2),
          catchup=False)


dates = 5



start = DummyOperator(
        task_id="start",
        trigger_rule="all_success",
        dag=dag
    )
task_arr = []
task_arr.append(start)
for i in range(dates):
    start_task = DummyOperator(
        task_id=f"start_{i}",
        trigger_rule="all_success",
        dag=dag
    )
    task_2 = DummyOperator(
        task_id=f"task_2_{i}",
        trigger_rule="all_success",
        dag=dag

    )
    task_3 = DummyOperator(
        task_id=f"task_3_{i}",
        trigger_rule="all_success",
        dag=dag

    )
    task_4 = DummyOperator(
        task_id=f"task_4_{i}",
        trigger_rule="all_success",
        dag=dag
    )

    task_arr[-1] >> start_task >> [task_2, task_3] >> task_4
    task_arr.append(task_4)

end = DummyOperator(
    task_id="end",
    trigger_rule="all_success",
    dag=dag
)

task_arr[-1] >> end




