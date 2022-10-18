def send_mail(query_results)-> csv.writer:
    from pathlib import Path
    Path("/home/airflow/gcs/data/").mkdir(parents=True, exist_ok=True) #create dir if doesnt exists
    
    
    with open('/home/airflow/gcs/data/email_attach.csv') as f:
        writer = csv.writer(f, delimiter=';')
        column_names = ["col1", "col_2", "col_3", "col_4", "col_5"]
        writer.writerow(column_names)
        for result in query_results:
            writer.writerow(["col1", "col_2", "col_3", "col_4", "col_5"])

    send_email(sender, to, """html""", ['/home/airflow/gcs/data/email_attach.csv'])

    os.remove('/home/airflow/gcs/data/email_attach.csv')

def get_column_names(..):
    ..

def prepare_mail_with_atachment(project_name, dataset_name, table_name):
    query = "SELECT * from {project_name}.{dataset_name}.{table_name}".format(project_name=project_name, dataset_name=dataset_name, table_name=table_name)
    query_job = bq_client.query(query)
    results = query_job.result()

    data = []
    columns = get_column_names(..)
    data.append(columns)

    for result in results:
        data.append([result[str(col)] for col in columns])

    html = """ check the table """
    send_mail_with_attachment(..)
