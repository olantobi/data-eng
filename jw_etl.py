import requests
import pandas as pd
import os
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import pymysql

# CONSTANT VALUES
OWNER = 'CSSEGISandData'
REPO = 'COVID-19'
PATH = 'csse_covid_19_data/csse_covid_19_daily_reports'
URL = f'https://api.github.com/repos/{OWNER}/{REPO}/contents/{PATH}'

DB_NAME = 'jw_etl'
DB_TABLE = 'covid_report'
DB_HOST = '127.0.0.1'
DB_USERNAME = 'root'
DB_PASSWORD = 'root'

# List of labels to be renamed
relabel = {
    # 'Last Update': 'Last_Update',
    'Country/Region': 'Country_Region',
    'Lat': 'Latitude',
    'Long_': 'Longitude',
    'Province/State': 'Province_State',
}

download_urls = []
response = requests.get(URL)

for data in tqdm(response.json()):
  if data['name'].endswith('.csv'):
    download_urls.append(data['download_url'])


def refactor_dataframe(dat, filename):
  """Refactor the dataframe to be uploaded into a SQL database
  as a pandas dataframe
  """
  print('Refactor dataframe and cleanup labels')

  # Rename labels
  for label in dat:
    if label in relabel:
      dat = dat.rename(columns = {label: relabel[label]})

  # return a dataframe with these parameters
  labels = ['Province_State', 'Country_Region', 'Last_Update', 'Confirmed', 'Deaths', 'Recovered']

  #filename is date
  if 'Last_Update' not in dat:
    dat['Last_Update'] = pd.to_datetime(filename)
  
  for label in labels:
    if label not in dat:
      dat[label] = float('nan')

  return dat[labels]


def upload_to_sql(filenames):
  """Given a list of paths, upload to database
  """
  db_url = f'mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
  sqlEngine = create_engine(db_url, pool_recycle=3600)
  dbConnection = sqlEngine.connect()

  for i, file_path in tqdm(list(enumerate(filenames))):
    dat = pd.read_csv(file_path)

    # Get filename
    filename = os.path.basename(file_path).split('.')[0]

    dat = refactor_dataframe(dat, filename)

    print('Uploading dataframe to database')
    try:
      dat.to_sql(DB_TABLE, dbConnection, index=False, if_exists='append');
    except ValueError as vx:
      print(vx)
    except Exception as ex:   
      print(ex)
    else:
      print("Data saved successfully in %s table."%DB_TABLE);   
  
  dbConnection.close()
  

upload_to_sql(download_urls)