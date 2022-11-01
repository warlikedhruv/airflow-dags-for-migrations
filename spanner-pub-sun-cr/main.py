import os
from flask import Flask, request
from utils import read_pub_sub_msg as queue_ops
from utils import  gcp_api_utils as spanner_ops
from utils import  push_pub_sub_msg as push_msg
from utils import download_and_push_gcs
app = Flask(__name__)

@app.route("/upload", methods=["POST"])
def push_file():
    project_id = ""
    bucket_name = ""
    headers = {"Authorization": "dXNlcg=="}
    api_url = "https://reqres.in/api/users"
    bucket_client = download_and_push_gcs.initialize_gcs_bucket_client(project_id, bucket_name)
    data = download_and_push_gcs.download_file_as_string(bucket_client, "file3.txt")
    download_and_push_gcs.upload_string_as_file(api_url, headers, data)

@app.route("/", methods=["POST"])
def process_response():
    data = queue_ops.read(request.get_json())
    print("SPANNER Data", data)
    try:
        if data is not None:
            gcs_operator = spanner_ops.GCS_Operators("data")
            gcs_operator.process_sub_files()
            spanner_ops.upload_to_cloud_spanner(INSTANCE_NAME="testspanner", DATABASE_NAME="TEST")
            pass
        push_msg.push_msg('data', project_id="astral-volt-359004", topic_id="successqueue")
        print("SPANNER DATA 2",data)
    except Exception as e:
        print("SPANNER Exception", str(e))
    return ("", 200)
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))