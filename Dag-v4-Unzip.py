from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile
import io


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


def zipextract():
    bucketname = ""
    zipfilename_with_path = ""
    storage_client = storage.Client() # impersonate it


    bucket = storage_client.get_bucket(bucketname) # ingress bucket name

    destination_blob_pathname = zipfilename_with_path

    blob = bucket.blob(destination_blob_pathname)
    zipbytes = io.BytesIO(blob.download_as_string())

    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                blob = bucket.blob(zipfilename_with_path + "/" + contentfilename)
                blob.upload_from_string(contentfile) # upload to same bucket

def helper_scan_bucket(prefix, regex):

    blobs = storage_client.list_blobs(prefix)
    for blob in blobs:
        if re.match(regex, blob.name):
            return str(blob.name) # return first file occured.  # new update
    return None


def find_master_file():
    prefix = ""
    regex = ""

    if helper_scan_bucket(prefix, regex) is None:
        return 'SendEmailTask'
    return "DummyForward"


def copy_zip_to_staging():
    prefix  = "" # same in the prefix of find master file
    regex = ""
    blob_path = helper_scan_bucket(prefix, regex)
    copy_file(blob_path) # same copy function in previous files
    return blob_path


def unzip_file_in_staging():
    bucket = "" # staging bucket name
    zipfilename_with_path = "" # for now put static file path to test
    destination_blob_pathname = "" # same as zipfilename path
    blob = bucket.blob(destination_blob_pathname)
    zipbytes = io.BytesIO(blob.download_as_string())

    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                blob = bucket.blob(zipfilename_with_path + "/" + contentfilename)
                blob.upload_from_string(contentfile)  # upload to same bucket


start = DummyOperator(
    task_id="start",
    trigger_rule="all_success",
    dag=dag
)

# scan the manifest files and insert into manifest stagging table
task_1 = PythonOperator(
    task_id='load_manifest_stagging_table',
    python_callable=load_manifest_stagging_table,
    dag=dag
)

# detect the new transaction_dt and history_dt and ignore future dates
task_2 = PythonOperator(
    task_id='load_control_table',
    python_callable=load_control_table,
    dag=dag)

end = DummyOperator(
    task_id="end",
    trigger_rule="all_success",
    dag=dag
)

start >> task_1 >> task_2 >> end


