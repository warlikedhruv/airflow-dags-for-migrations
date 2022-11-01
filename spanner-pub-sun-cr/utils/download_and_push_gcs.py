from google.cloud import storage

import io
import requests


def upload_string_as_file(api_url, headers, data_file):
    with io.StringIO() as f:
        f.write(data_file)
        files = {'file': f}

        r = requests.post(api_url, files=files, data={}, headers=headers)
        print(r.status_code, r.text)


# Initialise a client
def initialize_gcs_bucket_client(project_id, bucket_name):
    storage_client = storage.Client(project_id)
    bucket = storage_client.get_bucket(bucket_name)
    return bucket


def download_file_as_string(bucket, gcs_file_path):
    blob = bucket.blob("folder_one/foldertwo/filename.extension")
    # Download the file to a destination
    return blob.download_as_string(gcs_file_path)
