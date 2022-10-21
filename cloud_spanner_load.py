from google.cloud import spanner
import requests
import json
import io
from google.cloud import storage
class ApiGcsHandler:
    file_urls = []
    def __init__(self, api_url_1):
        self.api_url_1 = api_url_1
    
    def hit_api(self, url, header=None, payload=None):
        x = requests.post(url, header=header, payload=payload)
        return json.dumps(x.json)

    def get_file_urls(self, url):
        response = self.hit_api(url)
        return response.files

    def upload_file_to_gcs(self, bucket_name, file_data, file_name):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(file_data)


    def get_files(self):
        self.file_urls = get_file_urls(self.api_url_1)
        for url in self.file_urls:
            response = self.hit_api(url)
            self.upload_file_to_gcs("bucket_name", response.data, "name:"+str(url))

            


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


