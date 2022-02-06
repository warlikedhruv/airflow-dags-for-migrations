from google.cloud import storage
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
from airflow.utils.email import send_email_smtp

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/mineral-order-337219-e4d095ffa62f.json"  # do not use

"""
ENV VARIABLES
"""
BUCKET_NAME = "airflow-test-bucket-1107"
storage_client = storage.Client()
today_dt = datetime.now().date()
"""

"""
"""
STEP:1
"""


def scan_bucket():
    import re
    prefix = "manifest/"
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix, delimiter="/")
    yesterday_dt = today_dt - timedelta(2)
    kv_feed_regex = prefix + "auction_kv_labels_feed-11303-" + yesterday_dt.strftime("%Y%m%d") + "\w+"
    standard_feed_regex = prefix + "standard_feed_feed-11303-" + yesterday_dt.strftime("%Y%m%d") + "\w+"
    kv_feed = []
    standard_feed = []
    for blob in blobs:
        if re.match(kv_feed_regex, blob.name):
            kv_feed.append(blob.name)
        elif re.match(standard_feed_regex, blob.name):
            standard_feed.append(blob.name)

    return {"kv_feed": kv_feed, "standard_feed": standard_feed}


"""
STEP:2
"""


def filter_duplicate_hours(feed_path):
    filter_hours_dict = {}
    duplicate_file_list = []

    for filepath in feed_path:
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


def remove_duplicate_hours(**kwargs):
    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='scan_bucket')

    filtered_kv_feeds = filter_duplicate_hours(feed_files['kv_feed'])

    filtered_standard_feeds = filter_duplicate_hours(feed_files['standard_feed'])

    return {"kv_feed": filtered_kv_feeds, "standard_feed": filtered_standard_feeds}


"""
STEP - 3
"""


def prepared_data_manifest_stg(feed_type):
    valid_files = feed_type["processed_files"]
    duplicate_files = feed_type["duplicate_file_list"]
    values = []

    for file_path in valid_files:
        manifest_name = file_path.split("/", 1)[-1]
        transaction_dt = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").date()
        transaction_hr = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").hour
        file_valid_check = "N"
        load_dt = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").date() + timedelta(1)
        row = f"('{manifest_name}', '{transaction_dt}', '{transaction_hr}', '{file_valid_check}', '{load_dt}')"
        values.append(row)

    for file_path in duplicate_files:
        manifest_name = file_path.split("/", 1)[-1]
        transaction_dt = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").date()
        transaction_hr = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").hour
        file_valid_check = "Y"
        load_dt = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").date() + timedelta(1)
        row = f"('{manifest_name}', '{transaction_dt}', '{transaction_hr}', '{file_valid_check}', '{load_dt}')"
        values.append(row)

    return values


def load_manifest_table(**kwargs):
    from google.cloud import bigquery

    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='remove_duplicate_hours')

    insert_job_kv_feeds_values = prepared_data_manifest_stg(feed_files['kv_feed'])
    insert_kv_feeds_sql = "INSERT INTO `mineral-order-337219.test_dags.dummy_table` VALUES " + ",".join(
        insert_job_kv_feeds_values)
    print(insert_kv_feeds_sql)

    insert_job_kv_feeds_values = prepared_data_manifest_stg(feed_files['standard_feed'])
    insert_kv_feeds_sql = "INSERT INTO `mineral-order-337219.test_dags.dummy_table` VALUES " + ",".join(
        insert_job_kv_feeds_values)
    print(insert_kv_feeds_sql)

    # insert_client = bigquery.Client()
    # table_id_status = ""
    # job_config = bigquery.QueryJobConfig(labels={})
    # insert_job = insert_client.query("INSERT INTO `mineral-order-337219.test_dags.dummy_table` VALUES ('1', '2')",
    #                                  location="US", job_config=job_config)
    # insert_job.result()


"""
STEP-4
"""


def send_warning_email(**kwargs):
    send_email_smtp("test@email.com", "TEST", "BODY MESSAGE")


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

scan_bucket_t1_1 = PythonOperator(
    task_id='scan_bucket',
    python_callable=scan_bucket,
    provide_context=True,
    dag=dag)

duplicate_hour_check_t2_1 = PythonOperator(
    task_id='remove_duplicate_hours',
    python_callable=remove_duplicate_hours,
    provide_context=True,
    dag=dag)
#
load_manifest_table_t3_1 = PythonOperator(
    task_id='load_manifest_table',
    python_callable=load_manifest_table,
    provide_context=True,
    dag=dag)

scan_bucket_t1_1 >> duplicate_hour_check_t2_1 >> load_manifest_table_t3_1
