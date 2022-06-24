from google.oauth2 import service_account
from google.cloud import storage

from datetime import date
import time

import os
import gc

import pandas as pd

import sys

import logging
from importlib import reload
import traceback

import pyshorteners

#import local files
from credentials_file import aws_id, aws_secret
from slack_function import send_slack_message
from dictionaries import dict_list, translate_break, dtypes

os.environ['AWS_ID'] = aws_id
os.environ['AWS_SECRET'] = aws_secret

#name_file = os.path.basename(__file__)

patholens = '/home/ubuntu/marketing_etl/code/credentials/'
file_json = patholens + 'credentials_rappi.json'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = file_json
project_id = 'rappi-dependencies-project'

credential = service_account.Credentials.from_service_account_file(file_json)

current_time = time.localtime()
timestamp = time.strftime("%a %b %e %I:%M:%S %Y", current_time)
stamptime = str(timestamp)

def transform_dataframes(name_list: list, breakdown: str):
  today = date.today()
  time_stamp = today.strftime("%d-%m-%Y")

  type_tiny = pyshorteners.Shortener()
  
  path2 = 's3://rappi-bucket/googlesheet_raw_data/'
  path = "s3://rappi-bucket/tranform_data/"
  bucketnamevalue = '_at_'
  
  complete_dataframe = pd.DataFrame()


  
  for i in range(len(name_list)):
    print('file: ', path2 + name_list[i] + bucketnamevalue + str(time_stamp) + '.csv' #'gs://datalaketdh/' + path2 + name_list[i] + bucketnamevalue + str(time_stamp) + '.csv'
    )
    a = pd.read_csv(path2 + name_list[i] + bucketnamevalue + str(time_stamp) + '.csv', sep = ';', encoding = 'latin-1', dtype=dtypes, storage_options={'key': '{}'.format(aws_id),
                                      'secret': '{}'.format(aws_secret)})
    gc.collect()
    
    if (i>0) & ('events' in name_list[i]):
        complete_dataframe = pd.merge(complete_dataframe, a, how = 'left', on = 'Ad group ID')
    else:
        complete_dataframe = pd.concat([complete_dataframe, a])
    
    print('file: ', 'finish')

  complete_dataframe.columns = complete_dataframe.columns.str.replace('.', '')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('&', 'and')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('#', '_')
  complete_dataframe.columns = complete_dataframe.columns.str.replace(' ', '_')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('%', '_')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('(', '_')
  complete_dataframe.columns = complete_dataframe.columns.str.replace(')', '_')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('/', '_')
  complete_dataframe.columns = complete_dataframe.columns.str.replace(':', '_')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('-', '_')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('ó', 'o')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('á', 'a')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('í', 'i')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('é', 'e')
  complete_dataframe.columns = complete_dataframe.columns.str.replace('ú', 'u')
  
  complete_dataframe['upload_timestamp'] = stamptime

  complete_dataframe = complete_dataframe.fillna('0')

  if set(['interaction_rate_1']).issubset(complete_dataframe.columns):
    del complete_dataframe['interaction_rate_1']
  
  complete_dataframe['zoho_lead'] = complete_dataframe['zoholead']+complete_dataframe['zoholead1']
  complete_dataframe['sign_up'] = complete_dataframe['signup1']+complete_dataframe['signup']

  del complete_dataframe['zoholead']
  del complete_dataframe['signup1']
  del complete_dataframe['signup']
  del complete_dataframe['zoholead1']

  uniques_ad_creative_image_url = complete_dataframe[['ad_creative_image_url']].groupby('ad_creative_image_url', as_index=False).sum()
  uniques_ad_creative_thumbnail_url = complete_dataframe[['ad_creative_thumbnail_url']].groupby('ad_creative_thumbnail_url', as_index=False).sum()

  uniques_ad_creative_thumbnail_url['short_ad_creative_thumbnail_url'] = ''
  uniques_ad_creative_image_url['short_ad_creative_image_url'] = ''

  for item,url in enumerate(uniques_ad_creative_image_url.ad_creative_image_url):
    #print(item,url)
    if url == '0': 
      uniques_ad_creative_image_url['short_ad_creative_image_url'][item] = 'non-url'
    else:
      uniques_ad_creative_image_url['short_ad_creative_image_url'][item] = type_tiny.tinyurl.short(url)
    time.sleep(0.15)

  for item,url in enumerate(uniques_ad_creative_thumbnail_url.ad_creative_thumbnail_url):
    #print(item,url)
    if url == '0': 
      uniques_ad_creative_thumbnail_url['short_ad_creative_thumbnail_url'][item] = 'non-url'
    else:
      uniques_ad_creative_thumbnail_url['short_ad_creative_thumbnail_url'][item] = type_tiny.tinyurl.short(url)
    time.sleep(0.15)
  
  complete_dataframe_with = pd.merge(
      complete_dataframe,
      uniques_ad_creative_thumbnail_url,
      how = 'left',
      on = 'ad_creative_thumbnail_url'
  )

  complete_dataframe_with_shorts = pd.merge(
      complete_dataframe_with,
      uniques_ad_creative_image_url,
      how = 'left',
      on = 'ad_creative_image_url'
  )

  gc.collect()

  del complete_dataframe_with_shorts['ad_creative_image_url']
  del complete_dataframe_with_shorts['ad_creative_thumbnail_url']
  
  complete_dataframe_with_shorts.to_csv(path + 'tranform_data_' + breakdown + bucketnamevalue + str(time_stamp) +'.csv', sep = ';', index_label = False,
          storage_options={'key': '{}'.format(aws_id),
                           'secret': '{}'.format(aws_secret)})
  
  print('Saved dataframe with breakdown {B}'.format(B = breakdown))

if __name__ == '__main__':

    today = date.today()
    time_stamp = today.strftime("%d-%m-%Y-%H-%M")
    name = 'transform_compilation_files' + '_at_' + str(time_stamp) + '_' + str(sys.argv[1])
    
    reload(logging)

    LOG_FILENAME = r'/home/ubuntu/marketing_etl/code/logs/' + name + '.log'

    logging.basicConfig(filename = LOG_FILENAME , level = logging.DEBUG)

    f = open(LOG_FILENAME, 'rt')

    try:
        body = f.read()
        print("Your key is: ", sys.argv[0])
        transform_dataframes(name_list = dict_list[sys.argv[0]], breakdown = translate_break[sys.argv[0]])

    except Exception as e:
        logging.error("Exception occurred", exc_info = True)
        send_slack_message('Notificacion:large_red_circle:: El siguiente error evito la correcta ejecucion dentro del codigo: \n {} \n Puedes obtener mas detalles accediendo a {} dentro del servidor'.format(
            traceback.format_exc(),str(LOG_FILENAME)
             )
            )

    finally:
        f.close()
