from google.cloud import storage
from zipfile import ZipFile
from zipfile import is_zipfile
import io


def zipextract():
    bucketname = ""
    zipfilename_with_path = ""
    storage_client = storage.Client() # impersonate it


    bucket = storage_client.get_bucket(bucketname) # ingress bucket name

    destination_blob_pathname = zipfilename_with_path

    blob = bucket.blob(destination_blob_pathname)
    zipbytes = io.BytesIO(blob.download_as_string())

    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                blob = bucket.blob(zipfilename_with_path + "/" + contentfilename)
                blob.upload_from_string(contentfile) # upload to same bucket