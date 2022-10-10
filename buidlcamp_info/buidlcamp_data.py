'''
@author: huihao
'''
import datetime
import sys
sys.path.append('..')
import hashlib

from utils import sql_operations as SO
import datetime

import pandas as PD
from utils import constants as CT

def fetch_buidlcamp_info(database, user_name=None, ip_address=None):
    '''
    :param database:
    :param limit:
    :return:
    '''
    table_name= f'buidlcamp_table'
    my_connection, my_cursor = SO.connect_sql(database)
    sql_sentence = f"select * from {table_name} where user_name='{user_name}' and ip_address={ip_address}"
    my_cursor.execute(sql_sentence)
    final_result = PD.read_sql(sql_sentence,my_connection)
    return PD.DataFrame(final_result).values.tolist()

def into_buidlcamp_info(database, user_name, ip_address):
    '''
    :param database:
    :param limit:
    :param address:
    :return:
    '''
    raw_data = PD.DataFrame({
        'user_name': [user_name],
        'user_id': hashlib.sha224(bytes(user_name+str(datetime.datetime.now()).split(".")[0], encoding = "utf8")).hexdigest(),
        'ip_address': [ip_address],
        'created_time': [str(datetime.datetime.now()).split(".")[0]]
    })
    try:
        SO.upload_sql_records(database, f'buidlcamp_table', raw_data, False)
        return True
    except:
        return False
    
if __name__ == '__main__':

    database = CT.DATABASE
    into_buidlcamp_info(database,'huihao','ip_address')
    print(fetch_buidlcamp_info(database,'huihao','ip_address'))
