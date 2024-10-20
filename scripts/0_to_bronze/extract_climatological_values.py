""" 
Author: Javier Gamero
Mail: javier.gamero@lisdatasolutions.com

In this file we will extract the climatological values from AEMET API
"""

################################################################################
# UTILS 
################################################################################

# libraries
import os 
import pandas as pd
import requests 
from datetime import date, timedelta
from dotenv import dotenv_values

# paths
ROOT = os.getcwd()
DATA = os.path.join(ROOT, 'data')
BRONZE = os.path.join(DATA, 'bronze')

print(f'Root directory: {ROOT}')
print(f'Bronze directory: {BRONZE}')

# api key
api_key = dotenv_values()['APIKEY']

# aemet api requirements 
host = 'https://opendata.aemet.es/opendata/'
headers = {'api_key': api_key}
end_date = date.today().strftime("%Y-%m-%dT00:00:00UTC") # today
start_date = (date.today()- timedelta(14)).strftime("%Y-%m-%dT00:00:00UTC") 

################################################################################
# MAIN
################################################################################

endpoint = f'api/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/todasestaciones'

url = host + endpoint
print(url)
response = requests.get(url, headers=headers)
print(response.status_code, response.text)
download_url = response.json()['datos']
data = requests.get(download_url).json()

df = pd.DataFrame(data)
df.to_parquet(os.path.join(BRONZE, f'climatological_values_{start_date}-{end_date}.parquet'))
df.to_csv(os.path.join(BRONZE, f'climatological_values_{start_date}-{end_date}.csv'))