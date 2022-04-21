
def run_query(client, query):
    query_job = client.query(query)
    results = query_job.results()

    return results

def filter_duplicate_hr(feed_path)->dict:
    filter_hours_dict = {}
    duplicate_file_list = []

    for filepath in feed_path:
        try:
            file_split = filepath.split("-")
            key = file_split[0] + file_split[2]
            if key in filter_hours_dict:
                time_stamp_previous_file = int(filter_hours_dict[key][3].split(".")[0])
                time_stamp_current_file = int(file_split[3].split(".")[0])

                if time_stamp_previous_file < time_stamp_current_file:
                    duplicate_file_list.append("-".join(filter_hours_dict[key]))
                    filter_hours_dict[key] = file_split
                else:
                    duplicate_file_list.append(filepath)
            else:
                filter_hours_dict[key] = file_split
        except Exception as e:
            print("Exception:,", e)

    processed_files = []

    for key in filter_hours_dict.keys():
        processed_files.append("-".join(filter_hours_dict[key]))
    return {"processed_files": processed_files, "duplicate_file_list": duplicate_file_list}

def scan_and_load_file_to_table()->list:
    rows_to_insert = []
    prefix = "manifest/"
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix)
    blobs_list = [str(blob.name) for blob in blobs]
    filtered_files = filter_duplicate_hr(blobs_list)
    values_fmt_srt = "('{manifest_name}', '{transaction_dt}', '{transaction_hr}', '{duplicate_file_check}', '{load_ts}')"
    for file_path in filtered_files['processed_files']:
        manifest_name = file_path.split("/", 1)[-1]
        parsed_date_stamp = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H")
        transaction_dt = parsed_date_stamp.date()
        transaction_hr = parsed_date_stamp.hour
        duplicate_file_check = "N"
        rows_to_insert.append(values_fmt_srt.format(manifest_name=manifest_name,
                                                  transaction_dt=transaction_dt,
                                                  transaction_hr=transaction_hr,
                                                  duplicate_file_check=duplicate_file_check,
                                                  load_ts=load_ts))

    for file_path in filtered_files['duplicate_file_list']:
        manifest_name = file_path.split("/", 1)[-1]
        parsed_date_stamp = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H")
        transaction_dt = parsed_date_stamp.date()
        transaction_hr = parsed_date_stamp.hour
        duplicate_file_check = "N"
        rows_to_insert.append(values_fmt_srt.format(manifest_name=manifest_name,
                                                    transaction_dt=transaction_dt,
                                                    transaction_hr=transaction_hr,
                                                    duplicate_file_check=duplicate_file_check,
                                                    load_ts=load_ts))
    return rows_to_insert

def load_manifest_stagging_table():
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    #truncate_bq_table(client, "mineral-order-337219.test_dags.log_manifest_stg")

    rows = scan_and_load_file_to_table()
    insert_sql_query = "INSERT INTO TABLE `mineral-order-337219.test_dags.log_manifest_stg` VALUES "
    insert_sql_query += ",".join(rows)

    run_query(client, insert_sql_query)


"""
TASK 2
"""

def is_history_date(bq_client, transaction_dt):

    query = "SELECT transaction_dt, processed_flag from `mineral-order-337219.test_dags.control_table` " \
            "WHERE transaction_dt = '{transaction_dt}'".format(transaction_dt=transaction_dt)
    results = run_query(bq_client, query=query)
    if results.total_rows != 0:
        print("date Exists")
        return True
    return False


def change_control_table(bq_client, transaction_dt, processed_flag):
    dml_statement = (
        "UPDATE `mineral-order-337219.test_dags.control_table` \
        SET processed_flag = {processed_flag} \
        WHERE transaction_dt = '{transaction_dt}'".format(transaction_dt=transaction_dt,processed_flag=processed_flag))
    run_query(bq_client, dml_statement)


def load_control_table():
    from google.cloud import bigquery
    client = bigquery.Client()
    rows_insert_for_control_table = []
    insert_query = "INSERT INTO TABLE `mineral-order-337219.test_dags.control_table` VALUES "
    values_fmt_srt = "('{transaction_dt}', '{processed_flag}')"
    query = "SELECT count(filename) as count, transaction_dt \
            FROM `mineral-order-337219.test_dags.log_manifest_stg` \
            GROUP BY transaction_dt"

    results = run_query(client, query)
    for result in results:
        if result.count >= 48:
            rows_insert_for_control_table.append(values_fmt_srt.format(transaction_dt= result.transaction_dt,
                                                                       processed_flag="false"))
        elif is_history_date(client, result.transaction_dt):
            # update the processed flag to false to again process the history files
            change_control_table(client, result.transaction_dt, 'false')


    # insert new transaction dates into control table
    insert_query += ",".join(rows_insert_for_control_table)
    run_query(client, insert_query)


"""
TASK -3 
"""
def move_file(bucket_name: str, destination_bucket_name: str, source_files: list, destination_files: list):
    """Moves a blob from one bucket to another with a new name."""

    source_bucket = storage_client.bucket(bucket_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    for source_path, destination_path in zip(source_files, destination_files):
        source_blob = source_bucket.blob(source_path)

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_path
        )
        # source_bucket.delete_blob(source_path)

        print(
            "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )

def fetch_feed_file_from_archive(common_prefix: str):
    # find directories with date eg : feeds/standard-feed/20220202/*
    blobs = storage_client.list_blobs(ARCHIVAL_BUCKET, prefix=common_prefix, delimiter="/")
    file_path = [str(blob.name) for blob in blobs]

    # move files back to ingress bucket
    move_file(ARCHIVAL_BUCKET, BUCKET_NAME, source_files=file_path, destination_files=file_path)

    return True


def find_manifest_files_from_archive(history_date)->list:
    import re
    prefix = "manifest/"
    blobs = storage_client.list_blobs(ARCHIVAL_BUCKET, prefix=prefix)
    feeds = []

    kv_feed_regex = prefix + "auction_kv_labels_feed-11303-" + history_date.strftime("%Y%m%d") + "\w+"
    standard_feed_regex = prefix + "standard_feed_feed-11303-" + history_date.strftime("%Y%m%d") + "\w+"

    for blob in blobs:
        if re.match(kv_feed_regex, blob.name):
            feeds.append(blob.name)
        elif re.match(standard_feed_regex, blob.name):
            feeds.append(blob.name)

    return feeds

def restore_history_file(history_date):
    # restore manifest files
    manifest_feeds = find_manifest_files_from_archive(history_date)
    move_file(ARCHIVAL_BUCKET, BUCKET_NAME, source_files=manifest_feeds, destination_files=manifest_feeds)

    # restore kv feeds
    kv_feed_common_path = "feeds/kv_feed/{date}/".format(date=history_date.strftime("%Y%m%d"))
    fetch_feed_file_from_archive(kv_feed_common_path)

    # restore standard feeds
    kv_feed_common_path = "feeds/standard_feed/{date}/".format(date=yesterday_dt.strftime("%Y%m%d"))
    fetch_feed_file_from_archive(kv_feed_common_path)



start = DummyOperator(
    task_id="start",
    trigger_rule="all_success",
    dag=dag
)

# scan the manifest files and insert into manifest stagging table
task_1 = PythonOperator(
    task_id='load_manifest_stagging_table',
    python_callable=load_manifest_stagging_table,
    dag=dag
)

# detect the new transaction_dt and history_dt and ignore future dates
task_2 = PythonOperator(
    task_id='load_control_table',
    python_callable=load_control_table,
    dag=dag)

end = DummyOperator(
    task_id="end",
    trigger_rule="all_success",
    dag=dag
)