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

ENVIRONMENT = os.getenv("env").upper()
PARAMS_PATH = "/home/airflow/gcs/dags"


def read_parameters_from_file(filename: str, env: str):
    import yaml
    with open(filename, 'r') as file:
        params = yaml.full_load(file)
        my_config_dict = params[env]
        return my_config_dict


config_values = read_parameters_from_file(PARAMS_PATH + "/config.yml", ENVIRONMENT)
PIPELINE_PROJECT_ID = config_values['pipeline_project_id']
EMAIL = config_values['email']
SERVICE_ACC = config_values['service_account']
if 'date' in config_values.keys():
    today_dt = datetime.strptime(str(config_values['date']), '%d%m%Y')
else:
    today_dt = datetime.today().date()



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