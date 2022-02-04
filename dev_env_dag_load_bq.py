
"""
ENV VARIABLES
"""
BUCKET_NAME = "airflow-test-bucket-1107"


"""
STEP:1
"""

def list_manifest():
    from google.cloud import storage
    storage_client = storage.Client()
    # Note: Client.list_blobs requires at least package version 1.17.0.
    print("Blobs:")
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix="manifest/", delimiter="/")
    for blob in blobs:
        print(blob.name)
    if "/":
        print("Prefixes:")
        for prefix in blobs.prefixes:
            print(prefix)
