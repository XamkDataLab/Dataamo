import requests
import json
import time

def get_patent_data(start_date, end_date, token):
    url = 'https://api.lens.org/patent/search'
    token = token
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
                  "gte": "%s",
                  "lte": "%s"
                }
              }
            }
          ]
        }
      },
      "include": %s,
      "scroll": "1m",
      "size": 100
    }''' % (start_date, end_date, include)
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

    return json.dumps(data_out)

