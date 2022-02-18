def is_path_exists(bucket_name: str, files_list: list):
    missing_files = []
    for path in files_list:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(path)
        if not blob.exists():
            missing_files.append(path)
    if missing_files:
        missing_avro_files_failure_email(...) # same parameters
    return None

