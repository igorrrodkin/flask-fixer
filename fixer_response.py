import requests
import json
from config import access_key 

def latest_currencies():
    r_latest = requests.get('http://data.fixer.io/api/latest?access_key='+access_key)

    json_response = json.dumps(r_latest.json()['rates'] , indent=4 , sort_keys=True)

    currencies = json.loads(json_response).keys()
    prices = json.loads(json_response).values()
    response = []
    for currency, price in zip(currencies, prices):
        response.append({"currency": currency , "price": price})

    with open('latest.json', 'w+') as outfile:
                json.dump(response, outfile)
    return {'latest:':response}



def symbols_response():
    r_symbols = requests.get('http://data.fixer.io/api/symbols?access_key='+access_key)

    json_response = json.dumps(r_symbols.json()['symbols'] , indent=4 , sort_keys=True)

    currencies = json.loads(json_response).keys()
    names = json.loads(json_response).values()
    response = []
    for currency, name in zip(currencies, names):
        response.append({"currency": currency , "name": name})

    with open('symbols.json', 'w+') as outfile:
                json.dump(response, outfile)
    return {'symbols:':response}



    





