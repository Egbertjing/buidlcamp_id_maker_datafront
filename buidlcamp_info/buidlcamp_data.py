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

def fusion_buidlcamp_info(database, user_name=None, ip_address=None):
    '''
    :param database:
    :param limit:
    :return:
    '''
    table_name= CT.TABLE
    my_connection, my_cursor = SO.connect_sql(database)
    sql_sentence = f"select * from {table_name} where user_name='{user_name}' and ip_address='{ip_address}'"
    my_cursor.execute(sql_sentence)
    final_result = PD.read_sql(sql_sentence,my_connection)
    if(not final_result.empty):
        return PD.DataFrame(final_result).values.tolist()
    else:
        raw_data = PD.DataFrame({
            'user_name': [user_name],
            'user_id': (hashlib.sha224(bytes(user_name+str(datetime.datetime.now()).split(".")[0], encoding = "utf8")).hexdigest())[0:6],
            'ip_address': [ip_address],
            'address': '0x0000000000000000000000000000000000000000',
            'created_time': [str(datetime.datetime.now()).split(".")[0]]
        })
        SO.upload_sql_records(database, CT.TABLE, raw_data, False)
    sql_sentence = f"select * from {table_name} where user_name='{user_name}' and ip_address='{ip_address}'"
    my_cursor.execute(sql_sentence)
    final_result = PD.read_sql(sql_sentence,my_connection)
    return PD.DataFrame(final_result).values.tolist()

def fetch_buidlcamp_info(database, user_name=None, ip_address=None):
    '''
    :param database:
    :param limit:
    :return:
    '''
    table_name= CT.TABLE
    my_connection, my_cursor = SO.connect_sql(database)
    sql_sentence = f"select * from {table_name} where user_name='{user_name}' and ip_address='{ip_address}'"
    my_cursor.execute(sql_sentence)
    final_result = PD.read_sql(sql_sentence,my_connection)
    return PD.DataFrame(final_result).values.tolist()

def fetch_user_id(database):
    '''
    :param database:
    :param limit:
    :return:
    '''
    table_name= CT.TABLE
    my_connection, my_cursor = SO.connect_sql(database)
    sql_sentence = f"select user_id from {table_name};"
    my_cursor.execute(sql_sentence)
    final_result = PD.read_sql(sql_sentence,my_connection)
    return PD.DataFrame(final_result).to_json()

def into_buidlcamp_info(database, user_name, ip_address):
    '''
    :param database:
    :param limit:
    :param address:
    :return:
    '''
    table_name= CT.TABLE
    my_connection, my_cursor = SO.connect_sql(database)
    sql_sentence = f"select * from {table_name} where user_name='{user_name}' and ip_address='{ip_address}'"
    my_cursor.execute(sql_sentence)
    final_result = PD.read_sql(sql_sentence,my_connection)
    if(not final_result.empty):
        return False
    raw_data = PD.DataFrame({
        'user_name': [user_name],
        'user_id': (hashlib.sha224(bytes(user_name+str(datetime.datetime.now()).split(".")[0], encoding = "utf8")).hexdigest())[0:6],
        'ip_address': [ip_address],
        'address': '0x0000000000000000000000000000000000000000',
        'created_time': [str(datetime.datetime.now()).split(".")[0]]
    })
    try:
        SO.upload_sql_records(database, CT.TABLE, raw_data, False)
        return True
    except:
        return False
    
if __name__ == '__main__':

    database = CT.DATABASE
    print(fetch_user_id(database))
