import json
import requests

resp = {}
def function_post():
    api_response = json.loads(api_response)
    url = "http://google.com/data"
    data = {'keyword': keyword}
    token = data['response']['token']
    headers = {'Authorization': "Bearer {}".format(token)}
    r = requests.post(url,  headers=headers, data=data)
    print(r.text)
    r = json.loads(r.text)
    report_id = r['']['']
    return report_id

def download_data():
    report_id = "rows"
    url= "anyurl" + report_id
    response = requests.post(url)
    data = response.content
    print(data)


