import json
import requests

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
    response = x_Comm_var.xcomm_pull(task_id='function_post')  # new
    report_id = response['report_id']  # new
    token = response['token']  # new
    url = "https://google.com" + report_id
    print(url)  # new
    headers = {'Authorization': "Bearer {}".format(token)}  # new
    r = requests.get(url, headers=headers) # new
    data = r.text # new
    print(data)
    return True # new

    # report_id = "rows"
    # url = "anyurl" + report_id
    # response = requests.post(url)
    # data = response.content
    # print(data)
