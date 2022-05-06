import requests
import json

def latest_currencies():
    r_latest = requests.get('http://data.fixer.io/api/latest?access_key='+"ABC")

    json_response = json.dumps(r_latest.json()['rates'] , indent=4 , sort_keys=True)

    currencies = json.loads(json_response).keys()
    prices = json.loads(json_response).values()
    response = []
    for currency, price in zip(currencies, prices):
        response.append({"currency": currency , "price": price})

    with open('latest.json', 'w+') as outfile:
                json.dump(response, outfile)
    return {'latest:':response}