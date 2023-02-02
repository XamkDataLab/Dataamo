import requests
import json
import time

url = 'https://api.lens.org/patent/search'
token = 'HWUPWNqCnKL3OSATc2XfUuHSqUFT98xrLcTmPhEG2JOZjfuW49Kv'
include = '["lens_id","date_published","jurisdiction","biblio","doc_key","description","publication_type"]'
request_body = '''{
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "applicant.residence": "FI"
          }
        },
        {
          "range": {
            "date_published": {
              "gte": "2023-01-01",
              "lte": "2023-02-01"
            }
          }
        }
      ]
    }
  },
  "include": %s,
  "scroll": "1m",
  "size": 100
}''' % include
headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}

patents = []
scroll_id = None

while True:
    if scroll_id is not None:
        request_body = f'{{"scroll_id": "{scroll_id}","include": {include}}}'
    response = requests.post(url, data=request_body, headers=headers)
    if response.status_code == requests.codes.too_many_requests:
        print("TOO MANY REQUESTS, waiting...")
        time.sleep(8)
        continue
    if response.status_code != requests.codes.ok:
        print("ERROR:", response)
        break
    response = response.json()
    patents = patents + response['data']
    print(len(patents), "/", response['total'], "patenttia luettu...")
    if response['scroll_id'] is not None:
        scroll_id = response['scroll_id']
    print(len(patents) >= response['total'])
    if len(patents) >= response['total'] or len(response['data']) == 0:
        break
        
data_out = {"total": len(patents), "data": patents}

with open("patentdata.json", "w") as outfile:
    json.dump(data_out, outfile)
