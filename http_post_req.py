import json
import requests
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
resp = {}


def function_post():
    api_response = json.loads(api_response)
    url = "http://google.com/data"
    data = {'keyword': keyword}
    token = data['response']['token']
    headers = {'Authorization': "Bearer {}".format(token)}
    r = requests.post(url, headers=headers, data=data)
    print(r.text)
    r = json.loads(r.text)
    report_id = r['response']['report_id']
    return {'token': token, 'report_id': report_id}  # new


def download_data(**kwargs):
    x_Comm_var = kwargs['ti']
    response = x_Comm_var.xcomm_pull(task_ids='function_post')
    report_id = response['report_id']  # new
    token = response['token']  # new
    url = "https://google.com" + report_id
    print(url)  # new
    headers = {'Authorization': "Bearer {}".format(token)}
    r = requests.get(url, headers=headers)
    data = r.text
    print(data)
    return data

    # report_id = "rows"
    # url = "anyurl" + report_id
    # response = requests.post(url)
    # data = response.content
    # print(data)


def load_bq(**kwargs):
    x_Comm_var = kwargs['ti']
    data = x_Comm_var.xcomm_pull(task_ids='download_data')
    data_filter = list(filter(bool, data.splitlines()))
    columns = data_filter[0].split(",")
    values = data_filter[1].split(",")
    final_data = {}
    for key, value in zip(columns, values):
        final_data[key] = value
    sql_Statement = "INSERT INTO {schemaName} " \
                    "(database_name, table_name, transaction_dt, apn_impression_count, load_ts )" \
                    "select {database_name}, {table_name}, {transaction_dt}, {apn_impression_count}, {load_ts}"
    sql_Statement.format(database_name="",table_name="",
                         transaction_dt=final_data['day'],
                         apn_impression_count =final_data['imps']
                         , load_ts=datetime.now())
    bq_operator = BigQueryHook(bigquery_conn_id=bQ_CONN_ID,use_legacy_sql=False )
    service = bq_operator.get_service()
    run_query_operator = BigQueryBaseCursor(service=service, project_id="")
    run_query_operator.run_query(
        sql="",
        destination_dataset_table="",
        allow_large_results=True,
        use_legacy_sql=False
        write_disposition='WRITE_APPEND',

    )


