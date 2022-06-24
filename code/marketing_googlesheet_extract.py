import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account

import pandas as pd

from unidecode import unidecode

import os

import time
from datetime import date, timedelta

import logging
from importlib import reload
import traceback

#local python files
from credentials_file import aws_id, aws_secret
from slack_function import send_slack_message
from dictionaries import renamed_cols, googlesheets

patholens = '/home/ubuntu/marketing_etl/code/credentials/'
file_json = patholens + 'credentials_rappi.json'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = file_json

os.environ['AWS_ID'] = aws_id
os.environ['AWS_SECRET'] = aws_secret

project_id = 'rappi-dependencies-project'

credential = service_account.Credentials.from_service_account_file(file_json)

scope = ["https://spreadsheets.google.com/feeds",
            'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name(file_json, scope)

def tobase36(n):
    return(int(str(n), base = 36))

def extraction_data_saved(googlesheet_id: str, name_list: list):
  today = date.today() - timedelta(days=1)
  time_ref = today.strftime("%Y-%m-%d")

  today = date.today()
  time_stamp = today.strftime("%d-%m-%Y")

  client = gspread.authorize(creds).open(googlesheet_id)

  bucketnamevalue = '_at_'

  for i in range(len(name_list)-1):
    sheet = client.get_worksheet(i+1)
    m = sheet.get_all_values()

    columnas = m[0]
    c = columnas
    
    temporal = pd.DataFrame(m[1:], columns = c)

    if 'reach' not in name_list[i+1] and 'url' not in name_list[i+1] and 'form' not in name_list[i+1] and 'analytics' not in name_list[i+1]:
        temporal = temporal[~(temporal.Date == '')]
    
    if set(['Description']).issubset(temporal.columns):
      temporal.Description = temporal.Description.str.replace(' ', '')

    temporal.replace("\n", "", inplace = True)
    temporal.replace("\r", "", inplace = True)
    
    temporal.replace('', '0', inplace = True)
    temporal.replace('ê','e', inplace = True)
    temporal.replace('à','a', inplace = True)
    temporal.replace('â','a', inplace = True)
    temporal.replace('ã','a', inplace = True)
    temporal.replace('ç','c', inplace = True)
    temporal.replace('í','i', inplace = True)
    
    temporal = temporal.applymap(unidecode)
    
    temporal.rename(columns = renamed_cols, inplace = True) #colocar los rename de todo
    
    temporal['source'] = ''
    
    if ('google' or 'Google') in name_list[i+1]:

      temporal.rename(columns = {'Campaign name': 'campaign', 'Campaign ID': 'campaign_id'}, inplace = True)
      temporal['source'] = 'Google ads'
      
    elif ('facebook' or 'Facebook') in name_list[i+1]:
      temporal.rename(columns = {'Campaign name': 'campaign', 'Campaign ID': 'campaign_id'}, inplace = True)
      temporal['source'] = 'Facebook ads'

    temporal.columns = temporal.columns.str.lower()
    temporal = temporal.applymap(str) #
    temporal.fillna('0', inplace=True)
    temporal.replace('', '0', inplace = True)
    temporal.replace('-', '0', inplace = True)

    if 'reach' not in name_list[i+1] and 'url' not in name_list[i+1] and 'form' not in name_list[i+1] and 'analytics' not in name_list[i+1]:
      if max(temporal.date) != str(time_ref):
        send_slack_message('Notificacion:large_yellow_circle:: Las fechas en la dentro del tab {} son erroneas. Ve a la Google Sheet y refresca la consulta en SuperMetrics:recycle:'.format(
            str(name_list[i+1])
            )
        )
    
    else:
        pass

    temporal.to_csv('s3://rappi-bucket/googlesheet_raw_data/' + name_list[i+1]+ '_at_' + str(time_stamp) + '.csv', sep = ';', index_label = False,
                    storage_options={'key': '{}'.format(aws_id),
                                      'secret': '{}'.format(aws_secret)}
                    )
  print('Extraction of google sheets data.. / Done')

def compilation_data():

    extraction_data_saved(googlesheet_id = 'rappi_reports_supermetrics_etl', name_list = googlesheets['google_sheet1'])

if __name__ == '__main__':

    today = date.today()
    time_stamp = today.strftime("%d-%m-%Y-%H-%M")
    name = 'transform_googlesheets_files' + '_at_' + str(time_stamp)
    
    reload(logging)

    LOG_FILENAME = r'/home/ubuntu/marketing_etl/code/logs/' + name + '.log'

    logging.basicConfig(filename = LOG_FILENAME , level = logging.DEBUG)

    f = open(LOG_FILENAME, 'rt')

    try:
        body = f.read()

        compilation_data()

    except Exception as e:
        logging.error("Exception occurred", exc_info = True)
        send_slack_message('Notificacion:large_red_circle:: El siguiente error evito la correcta ejecucion dentro del codigo: \n {} \n Puedes obtener mas detalles accediendo a {} dentro del servidor'.format(
            traceback.format_exc(), str(LOG_FILENAME)
             )
            )
    
    finally:
        f.close()
