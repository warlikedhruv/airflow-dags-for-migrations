from google.cloud import spanner
import requests
import json
import io
from google.cloud import storage
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

            


def download_data_from_gcs():
    import csv
    from io import StringIO

    from google.cloud import storage

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(YOUR_BUCKET_NAME)

    blob = bucket.blob(YOUR_FILE_NAME)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8')

    blob = StringIO(blob)  #tranform bytes to string here
    names = csv.reader(blob)  #then use csv library to read the content
    return names

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False
    
def isinteger(value):
	try:
		int(value)
		return True
	except ValueError:
		return False
		
def insert_data(instance_id, database_id, table_id, batchsize, data, format_file):
    """Inserts sample data into the given database.
    The database and table must already exist and can be created using
    `create_database`.
    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    

    fmtreader = csv.reader(data)
    collist = []
    typelist = []
    icols = 0
    for col in fmtreader:
    	collist.append(col[1])
    	typelist.append(col[2])
    	icols = icols + 1
    	
    numcols = len(collist)
    
    ifile  = open(data_file, "r")
    reader = csv.reader(ifile,delimiter=',')
    alist = []
    irows = 0

    for row in reader:
        for x in range(0,numcols):
                if typelist[x] == 'integer':
                        row[x] = int(row[x])
                if typelist[x] == 'float':
                	row[x] = float(row[x])
                if typelist[x] == 'bytes':
                	row[x] = base64.b64encode(row[x])
        alist.append(row)
        irows = irows + 1
  		    		
    ifile.close()
    rowpos = 0
    batchrows = int(batchsize)
    while rowpos < irows:

            with database.batch() as batch:
                batch.insert(
                    table=table_id,
                    columns=collist,
                    values=alist[rowpos:rowpos+batchrows]
                    )

    		rowpos = rowpos + batchrows
    print 'inserted {0} rows'.format(rowpos)


data = download_data_from_gcs()
insert_data(...,data..,)



import flask
from flask_cors import cross_origin
from DATA_VALIDATION import validate
from DAO import mongo_utils as spark_mongo
from CONFIG.db_table_config import MongoDBTableConfig
import logging
import functions_framework


def send_response(status_code: int, message: str):
    return flask.jsonify({"status_code": status_code, message: message})


def get_data(data):
    data = data.get_json()
    if data and 'data' in data:
        return data
    raise ValueError('No data found in request')


def validate_data(data):
    return data


def filter_data(data):
    return data


def load_data(data) -> None:
    db = spark_mongo.MongoDB(MongoDBTableConfig())
    try:
        db.insert_data(data)
    except Exception as e:
        logging.critical("EXCEPTION IN INSERT", str(e))
        raise ValueError("EXCEPTION WHILE INSERTION IN DB", str(e))
    return None


data_pipeline = [get_data, validate_data, filter_data, load_data]


@cross_origin()
# @functions_framework.http
def xlab_data_fabric(request):
    data = request
    for step in data_pipeline:
        try:
            data = step(data)
        except ValueError as e:
            return send_response(403, str(e))
        except Exception as e:
            return send_response(401, str(e))
    return send_response(200, str("DATA LOADED TO DB"))













###################################### FINAL CODE #######################################
from google.cloud import spanner
import requests
import json
from google.cloud import storage
from enum import Enum
import logging

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
        response = ["https://reqres.in/api/users?page=2", "https://reqres.in/api/users/2", "https://reqres.in/api/unknown"]
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


import flask
from flask_cors import cross_origin
@cross_origin()
def process_request(request):
    url = request["url"]
    gcs_operator = GCS_Operators(url)
    gcs_operator.process_sub_files()
    return {"Data": str("DATA LOADED TO GCS")}

