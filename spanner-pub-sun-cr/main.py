import os
from flask import Flask, request
from utils import read_pub_sub_msg as queue_ops
from utils import  gcp_api_utils as spanner_ops
from utils import  push_pub_sub_msg as push_msg
app = Flask(__name__)


@app.route("/", methods=["POST"])
def process_response():
    data = queue_ops.read(request.get_json())
    if data is not None:
        gcs_operator = spanner_ops.GCS_Operators(data)
        gcs_operator.process_sub_files()
        spanner_ops.upload_to_cloud_spanner(INSTANCE_NAME="", DATABASE_NAME="")
        push_msg.push_msg('data', project_id="", topic_id="")

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))