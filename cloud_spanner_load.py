from google.cloud import spanner

def download_data_from_gcs():
    import csv
    from io import StringIO

    from google.cloud import storage

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(YOUR_BUCKET_NAME)

    blob = bucket.blob(YOUR_FILE_NAME)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8')

    blob = StringIO(blob)  #tranform bytes to string here
    names = csv.reader(blob)  #then use csv library to read the content
    return names

def isfloat(value):
  try:
    float(value)
    return True
  except ValueError:
    return False
    
def isinteger(value):
	try:
		int(value)
		return True
	except ValueError:
		return False
		
def insert_data(instance_id, database_id, table_id, batchsize, data, format_file):
    """Inserts sample data into the given database.
    The database and table must already exist and can be created using
    `create_database`.
    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)
    

    fmtreader = csv.reader(data)
    collist = []
    typelist = []
    icols = 0
    for col in fmtreader:
    	collist.append(col[1])
    	typelist.append(col[2])
    	icols = icols + 1
    	
    numcols = len(collist)
    
    ifile  = open(data_file, "r")
    reader = csv.reader(ifile,delimiter=',')
    alist = []
    irows = 0

    for row in reader:
        for x in range(0,numcols):
                if typelist[x] == 'integer':
                        row[x] = int(row[x])
                if typelist[x] == 'float':
                	row[x] = float(row[x])
                if typelist[x] == 'bytes':
                	row[x] = base64.b64encode(row[x])
        alist.append(row)
        irows = irows + 1
  		    		
    ifile.close()
    rowpos = 0
    batchrows = int(batchsize)
    while rowpos < irows:

            with database.batch() as batch:
                batch.insert(
                    table=table_id,
                    columns=collist,
                    values=alist[rowpos:rowpos+batchrows]
                    )

    		rowpos = rowpos + batchrows
    print 'inserted {0} rows'.format(rowpos)


data = download_data_from_gcs()
insert_data(...,data..,)
