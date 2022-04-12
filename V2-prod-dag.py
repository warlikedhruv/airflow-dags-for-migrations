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
          schedule_interval=None,
          dagrun_timeout=timedelta(seconds=2),
          catchup=False)

"""
INSERT INTO log_manifest_stg () VALUES 1,2,3
"""


def scan_and_load_file_to_table():
    insert_value_fmt = "({filepath}, {transaction_dt}),"
    rows_to_insert = []
    prefix = "manifest/"
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix)
    for blob in blobs:
        file_split = str(blob.name).split("/")
        file_split = list(filter(None, file_split))
        if len(file_split) > 1:
            transaction_dt = datetime.strptime(file_split[1].split("-")[2], "%Y%m%d%H") + timedelta(1)
            rows_to_insert.append({"filename": file_split[1], "transaction_dt": transaction_dt.strftime("%Y-%m-%d")})
    return rows_to_insert


def load_manifest_stagging_table():
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    rows = scan_and_load_file_to_table()
    print(rows)

    errors = client.insert_rows_json("mineral-order-337219.test_dags.log_manifest_stg", rows)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def load_control_table():
    from google.cloud import bigquery
    client = bigquery.Client()

    query = "SELECT count(filename) as count, transaction_dt \
            FROM `mineral-order-337219.test_dags.log_manifest_stg` \
            GROUP BY transaction_dt"

    query_job = client.query(query)
    results = query_job.result()
    for result in results:
        print(result.count, result.transaction_dt)


start = DummyOperator(
    task_id="start",
    trigger_rule="all_success",
    dag=dag
)

task1 = PythonOperator(
    task_id='load_control_table',
    python_callable=load_control_table,
    dag=dag)

end = DummyOperator(
    task_id="end",
    trigger_rule="all_success",
    dag=dag
)
