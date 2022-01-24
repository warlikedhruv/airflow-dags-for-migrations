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