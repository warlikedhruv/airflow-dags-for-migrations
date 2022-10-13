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