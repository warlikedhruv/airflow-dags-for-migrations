import re
from io import StringIO
import csv
def isFileCSV(filename):
    extension = filename.split(".")[-1]
    if extension.lower() == "csv":
        return True
    return False


def checkHeaderRow(header: str):
    error = []
    columns_names = header.split(",")
    if len(columns_names) != 6:
        pass  # error message
    if columns_names[1] != "":
        pass  # error message
    if columns_names[2] != "":
        pass  # eror message


def checkCell(cell_value: str, field_no: int, accepted_len: int, regex: str, is_alpha: bool, error: list):
    if len(cell_value) == 0:
        error.append("")
    elif len(cell_value) != accepted_len:
        error.append("")
    elif re.match(regex, cell_value):
        error.append("")
    return error


def checkRow(row: str):
    cell_values = row.split(",")
    error = []
    a = "a "
    a.strip()
    cell_values = [cell_value.strip for cell_value in cell_values]
    if len(cell_values) != 6:
        pass
    error = checkCell(cell_value=cell_values[0], field_no=1, accepted_len=3, regex=r"[a-zA-z][a-zA-Z]$",
                             is_alpha=True)

    error = checkCell(cell_value=cell_values[1], field_no=2, accepted_len=2, regex=r"\*\*$|[A-Za-z][A-Za-z]$",
                             is_alpha=True)
    error = checkCell(cell_value=cell_values[2], field_no=3, accepted_len=2,
                             regex=r"\*\*$|[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][A-Za-z0-9]$", is_alpha=True)
    error = checkCell(cell_value=cell_values[3], field_no=4, accepted_len=2,
                             regex=r"\*\*$|[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][A-Za-z0-9]$", is_alpha=True)
    error = checkCell(cell_value=cell_values[4], field_no=2, accepted_len=3,
                             regex=r"[A-Za-z][A-Za-z][A-Za-z]$", is_alpha=True)
    error = checkCell(cell_value=cell_values[5], field_no=2, accepted_len=2,
                             regex=r"[A-Za-z0-9][A-Za-z0-9]", is_alpha=True)
    error = checkCell(cell_value=cell_values[5], field_no=2, accepted_len=2,
                             regex=r"[Aa][Dd]", is_alpha=True)


def download_blob_into_memory(bucket_name, blob_name):
    """Downloads a blob into memory."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(blob_name)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8')
    blob = StringIO(blob)
    file_contents = csv.reader(blob, delimiter=',')

    print(
        "Downloaded storage object {} from bucket {} as the following string: {}.".format(
            blob_name, bucket_name, contents
        )
    )
