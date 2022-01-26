from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks import gcs_hook
from airflow.exceptions import AirflowSkipException
import logging
import json
import re

connection = gcs_hook.GoogleCloudStorageHook(google_cloud_storage_conn_id='my_gcp_connection')
logger = logging.getLogger("airflow.task")
default_args = {
    "owner": "dhruv-airflow-test",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("hook_gcp_bucket_final", default_args=default_args, schedule_interval=None)

"""
Supporting Functions
"""


def check_null_list(list_data):
    if len(list_data) < 1:
        return True
    return False


def pull_xComm_value(xComm, task_id):
    xComm_value = xComm.xcom_pull(task_ids=task_id)
    return xComm_value


"""
STEP-1
"""


def read_bucket_manifest_files(**kwargs):
    bucket_files_list = connection.list('airflow-test-bucket-1107', prefix="manifest/"
                                        , delimiter=".json")

    manifest_json_files_list = [file_path for file_path in bucket_files_list if len(list(filter(None, file_path.split("/")))) == 2 and "json" in file_path]
    if check_null_list(manifest_json_files_list):
        raise AirflowSkipException
    return manifest_json_files_list


"""
STEP-2
"""


def filter_manifest_files(**kwargs):
    xComm_var = kwargs['ti']
    manifest_json_files_list = xComm_var.xcom_pull(task_ids='read_bucket_manifest_files')
    filter_hours_dict = {}
    corrupt_file_list = []
    for file in manifest_json_files_list:
        try:
            file_split = file.split("-")
            key = file_split[2]
            if key in filter_hours_dict.keys():
                second_time_stamp_previous = int(filter_hours_dict[key][3].split(".")[0])
                second_time_stamp_current = int(file_split[3].split(".")[0])

                if second_time_stamp_previous < second_time_stamp_current:
                    corrupt_file_list.append("-".join(filter_hours_dict[key]))
                    filter_hours_dict[key] = file_split
                else:
                    corrupt_file_list.append(file)
            else:
                filter_hours_dict[file_split[2]] = file_split
        except Exception as e:

            logger.error(str(e))
    filtered_list = []
    for key in filter_hours_dict.keys():
        filtered_list.append("-".join(filter_hours_dict[key]))
    return {'filtered_list': filtered_list, 'corrupt_file_list': corrupt_file_list}


"""
Step-3.1 and Step-3.2
"""


def archive_corrupted_json_files(**kwargs):
    bucket_name = 'airflow-test-bucket-1107'
    xComm_var = kwargs['ti']
    valid_avro_file = xComm_var.xcom_pull(task_ids='filter_manifest_files')
    valid_avro_files_archive = valid_avro_file['corrupt_file_list']

    source_bucket = "airflow-test-bucket-1107"
    destination_bucket = "airflow-test-bucket-1107"
    common_file_path = "manifest/Archive/Corrupted_Files/"
    for file_path in valid_avro_files_archive:
        file_name = file_path.split("/")[-1]
        connection.copy(
            source_bucket=source_bucket,
            source_object=file_path,
            destination_bucket=destination_bucket,
            destination_object=common_file_path + file_name
        )
        # connection.delete(
        #     bucket_name = "airflow-test-bucket-1107",
        #     object_name = file
        # )


def process_manifest_json_file(**kwargs):
    xComm_var = kwargs['ti']
    manifest_json_files = xComm_var.xcom_pull(task_ids='filter_manifest_files')
    manifest_json_files_list = manifest_json_files['filtered_list']

    yesterday_date = (datetime.now() - timedelta(1)).date()

    avro_files_path = []
    for file_path in manifest_json_files_list:
        try:

            file_dt = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").date()

            if yesterday_date <= file_dt <= datetime.now().date():
                json_file_data = json.loads(
                    connection.download(bucket_name="airflow-test-bucket-1107", object_name=file_path))
                # json_file_data = json.loads(json_file_data)
                path = json_file_data['path'][1:]
                avro_files_path.extend([{json_file_data['feed_type']: path + "/" + filepath['name']} for filepath in
                                        json_file_data['files']])
            else:
                print(file_path, yesterday_date, file_dt)

        except Exception as e:
            logger.error(str(e))
    print(avro_files_path)
    if check_null_list(avro_files_path):
        raise AirflowSkipException
    return avro_files_path


read_bucket_files_t1 = PythonOperator(
    task_id='read_bucket_manifest_files',
    python_callable=read_bucket_manifest_files,
    provide_context=True,
    dag=dag)

filter_manifest_files_t2 = PythonOperator(
    task_id='filter_manifest_files',
    python_callable=filter_manifest_files,
    provide_context=True,
    dag=dag)



archive_corrupted_json_files_3_1 = PythonOperator(
    task_id='archive_corrupted_json_files',
    python_callable=archive_corrupted_json_files,
    provide_context=True,
    dag=dag)

process_manifest_json_file_3_2 = PythonOperator(
    task_id='process_manifest_json_file',
    python_callable=process_manifest_json_file,
    provide_context=True,
    dag=dag)

read_bucket_files_t1 >> filter_manifest_files_t2 >> [archive_corrupted_json_files_3_1, process_manifest_json_file_3_2]
