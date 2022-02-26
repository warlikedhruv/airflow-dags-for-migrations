# def is_path_exists(bucket_name: str, files_list: list):
#     missing_files = []
#     for path in files_list:
#         bucket = storage_client.get_bucket(bucket_name)
#         blob = bucket.blob(path)
#         if not blob.exists():
#             missing_files.append(path)
#     if missing_files:
#         missing_avro_files_failure_email(...)  # same parameters
#     return None
#

"""
Import Ymal Files and environment
"""
from datetime import datetime
import os

# ENVIRONMENT = os.getenv("env").upper()
# PARAMS_PATH = "/home/airflow/gcs/dags"


# def read_parameters_from_file(filename: str, env: str):
#     import yaml
#     with open(filename, 'r') as file:
#         params = yaml.full_load(file)
#         my_config_dict = params[env]
#         return my_config_dict
#
#
# config_values = read_parameters_from_file(PARAMS_PATH + "/config.yml", ENVIRONMENT)
# PIPELINE_PROJECT_ID = config_values['pipeline_project_id']
# EMAIL = config_values['email']
# SERVICE_ACC = config_values['service_account']
# if config_values['date']:
#     today_dt = datetime.strptime(str(config_values['date']), '%d%m%Y')
# else:
#     today_dt = datetime.today().date()



# def city_warning_email(environment, email_type, result_rows, dag_name="test_1", product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
#     email_subject = f"{environment}\t {email_type}\t for {product}\t {assets_tag}\t {dag_name}"
#     email_body = "<h2><p style='color:#FF0000'>***This email has been generated automatically - DO NOT REPLY Directly ***</p></h2>"
#     email_body = "<strong>Warning</strong>: One or more unmapped page codes were added to promospots_ads.server table in today; <br/></br>"
#     email_body += "<strong>Corresponding page code(s):</strong></br></br>"
#     email_body += "<table>"
#     #NEW
#     for rows in result_rows:
#         email_body +="<tr>"
#         for cell in rows:
#             email_body += f"<td>{cell}</td>"
#         email_body += "</tr>"
#     email_body += "</table></br></br>"
#
#     email_body += f"<strong>Action: </strong> REview with business team to check..... "
#
#     send_warning_email(recipent_email="", subject=email_subject, body=email_body)
#
#
# def run_query_function(**kwargs):
#     from google.cloud import bigquery
#     client = bigquery.Client()
#     query_job = client.query(
#         """
#         SELECT
#           CONCAT(
#             'https://stackoverflow.com/questions/',
#             CAST(id as STRING)) as url,
#           view_count
#         FROM `bigquery-public-data.stackoverflow.posts_questions`
#         WHERE tags like '%google-bigquery%'
#         ORDER BY view_count DESC
#         LIMIT 10"""
#     )
#
#     results = query_job.result()
#     result_rows = []
#     for row in results:
#         result_rows.append([row.col1, row.col2])
#     if result_rows:
#         city_warning_email(environment="", email_type="", result_rows=result_rows, dag_name="test_1", product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD")


# str1 = None
# if str1:
#     print("True")
# else:
#     print("fasle")


"""
BUG FIX AVRO FILE CHECK
"""

def is_path_exists(bucket_name: str, files_list: list):
    missing_avro_files = []
    valid_avro_files = files_list.copy() # new
    for path in files_list:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(path)
        if not blob.exists():
            missing_avro_files.append(path)
            valid_avro_files.remove(path) # new
    return valid_avro_files, missing_avro_files

def check_valid_avro_files(**kwargs):
    xComm_var = kwargs['ti']
    kv_feed_avro_files = xComm_var.xcomm_pull(key='kv_feed_avro_file', task_ids='prepare_avro_files')
    standard_feed_avro_files = xComm_var.xcomm_pull(key='standard_feed_avro_file', task_ids='prepare_avro_files')

    kv_feed_valid_avro_files, kv_feed_missing_avro_files  = is_path_exists(bucket_name=INGRESS_BUCKET_NAME, files_list=kv_feed_avro_files)
    standard_feed_valid_avro_files, standard_feed_missing_avro_files = is_path_exists(bucket_name=INGRESS_BUCKET_NAME,files_list=standard_feed_avro_files)

    total_missing_avro_files = kv_feed_missing_avro_files + standard_feed_missing_avro_files
    if total_missing_avro_files: # replace from is_path_exists function
        missing_avro_files_failure_email(.......)

    xComm_var.xcom_push(key='kv_feed_avro_file', value=kv_feed_valid_avro_files)
    xComm_var.xcom_push(key="standard_feed_avro_file", value=standard_feed_valid_avro_files)


from airflow.operators.dagrun_operator import TriggerDagRunOperator
trigger = TriggerDagRunOperator(
    task_id="trigger_dag_2",
    trigger_dag_id="external_task_sensor_dag_2",
    dag=dag,
)


