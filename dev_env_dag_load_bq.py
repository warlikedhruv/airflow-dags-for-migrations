from google.cloud import storage
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/mineral-order-337219-e4d095ffa62f.json"

"""
ENV VARIABLES
"""
BUCKET_NAME = "airflow-test-bucket-1107"
storage_client = storage.Client()

"""

"""
"""
STEP:1
"""


def scan_bucket_for_manifest_files():
    prefix = "manifest/"
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix, delimiter="/")
    manifest_json_files_list = [str(blob.name) for blob in blobs if
                                len(list(filter(None, str(blob.name).split("/")))) == 2]

    return {"manifest_json_files_list": manifest_json_files_list}


def get_yesterday_files(**kwargs):
    manifest_files = scan_bucket_for_manifest_files()['manifest_json_files_list']

    # getting yesterday's date
    today_dt = datetime.now().date()
    yesterday_dt = today_dt - timedelta(1)

    # saving yesterday and old files paths
    yesterday_json_files = []
    old_json_files = []

    for file_path in manifest_files:
        try:
            file_dt = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").date()
            print(file_dt)
            if yesterday_dt == file_dt:
                yesterday_json_files.append(file_path)
            elif file_dt < yesterday_dt:
                print("old file", file_path)
                old_json_files.append(file_path)
        except Exception as e:
            print(str(e))

    return {"yesterday_json_files": yesterday_json_files, "old_json_files": old_json_files}

"""
Step-2
"""
def duplicate_hour_check(**kwargs):
    xComm_var = kwargs['ti']
    yesterday_json_files = xComm_var.xcom_pull(task_ids="get_yesterday_files")['yesterday_json_files']

    filter_hours_dict = {}
    duplicate_file_list = []

    for filepath in yesterday_json_files:
        try:
            file_split = filepath.split("-")
            key = file_split[2]
            if key in filter_hours_dict:
                time_stamp_previous_file = int(filter_hours_dict[key][3].split(".")[0])
                time_stamp_current_file = int(file_split[3].split(".")[0])

                if time_stamp_previous_file < time_stamp_current_file:
                    duplicate_file_list.append("-".join(filter_hours_dict[key]))
                    filter_hours_dict[key] = file_split
                else:
                    duplicate_file_list.append(filepath)
            else:
                filter_hours_dict[file_split[2]] = file_split
        except Exception as e:
            print("Exception:,", e)

    processed_files = []
    for key in filter_hours_dict.keys():
        processed_files.append("-".join(filter_hours_dict[key]))
    return {"processed_files": processed_files, "duplicate_file_list": duplicate_file_list}


"""
STEP - 3
"""
def load_manifest_table(**kwargs):
    from google.cloud import bigquery
    insert_client = bigquery.Client()
    table_id_status = ""
    job_config = bigquery.QueryJobConfig(labels= {})
    insert_job = insert_client.query("INSERT INTO `mineral-order-337219.test_dags.dummy_table` VALUES ('1', '2')", location="US", job_config=job_config)
    insert_job.result()





default_args = {
    "owner": "dhruv-airflow-test-dev",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("hook_gcp_bucket_final_dev", default_args=default_args, schedule_interval=None)

read_bucket_files_t1_1 = PythonOperator(
    task_id='get_yesterday_files',
    python_callable=get_yesterday_files,
    provide_context=True,
    dag=dag)

duplicate_hour_check_t2_1 = PythonOperator(
    task_id='duplicate_hour_check',
    python_callable=duplicate_hour_check,
    provide_context=True,
    dag=dag)

load_manifest_table_t3_1 = PythonOperator(
    task_id='load_manifest_table',
    python_callable=load_manifest_table,
    provide_context=True,
    dag=dag)



read_bucket_files_t1_1 >> duplicate_hour_check_t2_1 >> load_manifest_table_t3_1
