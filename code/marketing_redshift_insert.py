from sqlalchemy import create_engine
import pandas as pd

from datetime import date
import time

import os
import sys

import gc

import logging
from importlib import reload
import traceback

#import local files
from credentials_file import aws_id, aws_secret, username, password, Host, Port, Database
from slack_function import send_slack_message
from dictionaries import stages_table, translate_break, dtypes,integer_dict,float_dict,string_dict

os.environ['AWS_ID'] = aws_id
os.environ['AWS_SECRET'] = aws_secret

#name_file = os.path.basename(__file__)

def integer_values(dfa, lista):
  return dfa.astype(lista)

def data_to_redshift(breakdown: str, table_name: str, list_of_intenger: list, list_of_floats64: list, list_of_strings: str):
  
  current_time = time.localtime()
  timestamp = time.strftime("%a %b %e %I:%M:%S %Y", current_time)
  stamptime = str(timestamp)
    
  print('Redshift start')

  path = 'tranform_data/'
  bucketnamevalue = '_at_'
  
  today = date.today()
  time_stamp = today.strftime("%d-%m-%Y")

  print('s3://my-bucket/' + path + 'tranform_data_' + breakdown + bucketnamevalue + str(time_stamp) + '.csv')
  dataframe_table = pd.read_csv('s3://my-bucket/' + path + 'tranform_data_' + breakdown + bucketnamevalue + str(time_stamp) + '.csv',
                                sep = ';', dtype=dtypes, storage_options={'key': '{}'.format(aws_id),
                                      'secret': '{}'.format(aws_secret)})

  dataframe_table.fillna('0', inplace=True)
  dataframe_table.replace('', '0', inplace = True)
  dataframe_table.replace('-', '0', inplace = True)

  dataframe_table['upload_timestamp'] = stamptime

  list_of_intenger = dict.fromkeys(list_of_intenger, int)
  dataframe_table = integer_values(dataframe_table, list_of_intenger)
  print('int ready')

  list_of_floats64 = dict.fromkeys(list_of_floats64, float)
  dataframe_table = integer_values(dataframe_table, list_of_floats64)
  print('float ready')

  list_of_strings = dict.fromkeys(list_of_strings, str)
  dataframe_table = integer_values(dataframe_table, list_of_strings)
  print('str ready')

  print('pass 2.1')

  if set(['date']).issubset(dataframe_table.columns):
    print('pass 2.1')
    dataframe_table = dataframe_table[dataframe_table.date != '0'].fillna('0')
    dataframe_table.reset_index(inplace = True, drop = True)

  dataframe_table.columns = dataframe_table.columns.str.replace('.', '_', regex=False)
  dataframe_table.columns = dataframe_table.columns.str.replace(':', '_', regex=False)

  if set(['ad_id']).issubset(dataframe_table.columns):
    dataframe_table.ad_id = dataframe_table.ad_id.replace('.0', '')

  conn_string = 'postgresql://{user}:{passw}@{host}:{port}/{database}'.format(user = username,
                                                                           passw = password,
                                                                           host = Host,
                                                                           port = Port,
                                                                           database = Database)
  
  eng = create_engine(conn_string)
  connection = eng.connect()

  dataframe_table.to_sql(table_name, connection, index = False, if_exists='append')
  
  gc.collect()
  time.sleep(3)
  
  print('redshift done')

if __name__ == '__main__':

    today = date.today()
    time_stamp = today.strftime("%d-%m-%Y-%H-%M")
    name = 'redshift_uploading' + '_at_' + str(time_stamp) + '_'
    
    reload(logging)

    LOG_FILENAME = r'/home/ubuntu/marketing_etl/code/logs/' + name + str(sys.argv[0]) + '.log'

    logging.basicConfig(filename = LOG_FILENAME , level = logging.DEBUG)

    f = open(LOG_FILENAME, 'rt')

    try:
        body = f.read()

        data_to_redshift(breakdown = translate_break[sys.argv[0]], 
                            table_name = stages_table[sys.argv[0]], 
                            list_of_intenger = integer_dict[sys.argv[0]], 
                            list_of_floats64 =float_dict[sys.argv[0]], 
                            list_of_strings = string_dict[sys.argv[0]]
                            )

    except Exception as e:
        logging.error("Exception occurred", exc_info = True)
        send_slack_message('Notificacion:large_red_circle:: El siguiente error evito la correcta ejecucion dentro del codigo: \n {} \n Puedes obtener mas detalles accediendo a {} dentro del servidor'.format(
            traceback.format_exc(), str(LOG_FILENAME)
             )
            )
    
    finally:
        f.close()