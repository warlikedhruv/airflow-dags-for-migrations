def is_path_exists(bucket_name: str, files_list: list):
    missing_files = []
    for path in files_list:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(path)
        if not blob.exists():
            missing_files.append(path)
    if missing_files:
        missing_avro_files_failure_email(...)  # same parameters
    return None


"""
Import Ymal Files and environment
"""
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