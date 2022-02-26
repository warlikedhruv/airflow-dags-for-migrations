from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
#from airflow.operators.sensors import ExternalTaskSensor for old
from airflow.sensors.external_task_sensor import ExternalTaskSensor


default_args = {
    "owner": "dhruv",
    "depends_on_past": False,
    "start_date": datetime(2022, 2, 25),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG('external_task_sensor_dag_2',
          schedule_interval= '58 21 * * *',
          default_args=default_args,
          catchup=False
          )

external_task_sensor = ExternalTaskSensor(
    task_id='external_task_sensor',
    poke_interval=60,
    timeout=180,
    soft_fail=False,
    retries=2,
    external_task_id='archive_duplicate_files',
    external_dag_id='hook_gcp_bucket_final_dev',
    allowed_states=['success'],
    execution_delta=timedelta(minutes=-1),
    dag=dag)


def my_processing_func(**kwargs):
    print("I have sensed the task is complete in a dag")


some_task = PythonOperator(
    task_id='task_1',
    python_callable=my_processing_func,

    dag=dag)

some_task