import requests
import json
#from config import access_key 
from logger import Logger

log = Logger('info.log').logger()

class Fixer_Io:
    def __init__(self , access_key):
        self.access_key  = access_key

    def request_fixerio(self ,fixer_endopoint, column_name , output_file):
        try:
            response = requests.get(f'http://data.fixer.io/api/{fixer_endopoint}?access_key={self.access_key}')
            if fixer_endopoint == 'latest':
                json_response = json.dumps(response.json()['rates'] , indent=4 , sort_keys=True)
            if fixer_endopoint == 'symbols':
                json_response = json.dumps(response.json()['symbols'] , indent=4 , sort_keys=True)
            currencies = json.loads(json_response).keys()
            data = json.loads(json_response).values()
            response = []
            for currency, data in zip(currencies, data):
                response.append({"currency": currency , column_name: data})

            with open(output_file, 'w+') as outfile:
                        json.dump(response, outfile)
            return log.info('Succefully loaded')


        except UnboundLocalError:
            return log.error('invalid fixer endpoint')
        except KeyError:
            return log.error('access key isnot valid , bad request')
        except:
            return log.error('something has happened')

# fix = Fixer_Io(access_key)
# fix.request_fixerio('latest' , 'price' , 'datalatest.json')



    





