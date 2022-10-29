from google.cloud import spanner
import requests
import json
from google.cloud import storage
from enum import Enum
import logging
from google.cloud.spanner import KeySet
from google.cloud import spanner


class ApiGcsHandler:
    file_urls = []
    headers = None
    payload = None

    def __init__(self, api_url_1):
        self.api_url_1 = api_url_1

    def hit_api(self, url, headers=None, payload=None):
        x = requests.get(url, headers=headers, data=payload)
        print(x.text)
        return json.loads(x.text)

    def get_file_urls(self, url):
        response = self.hit_api(url, self.headers, self.payload)
        return response

    def upload_file_to_gcs(self, bucket_name, file_data, file_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(file_data)

    def get_files(self):
        self.file_urls = self.get_file_urls(self.api_url_1)
        print(self.file_urls)
        for url in self.file_urls:
            self.set_headers({"Access-Control-Allow-Origin", "*"})
            response = self.hit_api(url["url"])
            print(response)
            self.upload_file_to_gcs("bucket_name", response.data, "name:" + str(url))

    def set_headers(self, headers):
        self.headers = headers


class Api_Operators:
    def hit_get_request(self, url, headers=None, data=None):
        return json.loads(requests.get(url, headers=headers, data=None).text)

    def hit_post_request(self, url, headers=None, data=None):
        return json.loads(requests.post(url, headers=headers, data=data).text)


class GCS_Buckets(Enum):
    TEST_SPANNER_BUCKET = "test_spanner_bucket"

    def __str__(self) -> str:
        return str(self.value)


class GCS_Operators:
    def __init__(self, main_url):
        self.api_operator = Api_Operators()
        self.main_url = main_url

    def get_file_urls(self, url):
        # response = self.api_operator.hit_get_request(url)
        # for testing
        response = ["https://reqres.in/api/users?page=2", "https://reqres.in/api/users/2",
                    "https://reqres.in/api/unknown"]
        for file_url in response:
            yield file_url

    def upload_file_to_gcs(self, bucket_name, file_data, file_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(file_data)

    def process_sub_files(self):
        file_url_generator = self.get_file_urls(self.main_url)

        for url in file_url_generator:
            response = self.api_operator.hit_get_request(url)
            print(response)
            self.upload_file_to_gcs(str(GCS_Buckets.TEST_SPANNER_BUCKET), response.data, "name:" + str(url))
            logging.info("UPLOADED FILE TO ", str(GCS_Buckets.TEST_SPANNER_BUCKET), " FILE NAME ", "name:", str(url))


def upload_to_cloud_spanner(INSTANCE_NAME, DATABASE_NAME):
    client = spanner.Client()
    instance = client.instance(INSTANCE_NAME)
    database = instance.database(DATABASE_NAME)

    with database.batch() as batch:
        batch.insert(
            'citizens', columns=['email', 'first_name', 'last_name', 'age'],
            values=[
                ['phred@exammple.com', 'Phred', 'Phlyntstone', 32],
                ['bharney@example.com', 'Bharney', 'Rhubble', 31],
            ])
