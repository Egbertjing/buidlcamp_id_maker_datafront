import sys
sys.path.append("..")

import psycopg2, datetime
import pandas as PD

from utils import constants as CT

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def build_database(database=None) -> None:
    con = psycopg2.connect(
        user=CT.USER,
        password=CT.PASSWORD,
        host=CT.HOST,
        port=CT.PORT
    )

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cursor = con.cursor()

    sql_create_database = "create database " + database + ";"
    try:
        cursor.execute(sql_create_database)
        print(f'*** {database} successfully build ***')
    except:
        print('Fail to build DATABASE')
        sys.exit()

def connect_sql(database, do_print=False):
    """
    connect_sql
    :param do_print: whether print or not
    :return: a connection or not
    """
    try:
        my_connection = psycopg2.connect(
            database=database,
            user=CT.USER,
            password=CT.PASSWORD,
            host=CT.HOST,
            port=CT.PORT
        )
        if do_print:
            print('*** SQL successfully connected ***')
        my_cursor = my_connection.cursor()
    except Exception as e:
        print('Fail to connect MySQL', e)
        sys.exit()
    return my_connection, my_cursor

def disconnect_sql(my_connection, my_cursor, do_print=False) -> None:
    """
    disconnect sql
    :param do_print:
    :param my_connection: connection
    :param my_cursor: cursor
    :return: no return
    """
    my_connection.commit()
    my_cursor.close()
    my_connection.close()
    if do_print:
        print('*** SQL successfully closed ***')

def table_exits(database, table) -> bool:
    '''
    whether table exits in database
    :param database:
    :param table:
    :return:
    '''
    my_connection, my_cursor = connect_sql(database)
    my_cursor.execute(f"select count(*) from pg_class where relname = '{table}';")
    result = my_cursor.fetchall()
    try:
        if result[0][0] == 1:
            return True
        return False
    except:
        return False

def upload_sql_records(database, table_name, newest_result, drop=False) -> None:
    """
    upload new results to SQL
    :param database: which database to upload the data
    :param drop:
    :param table_name: which table to locate the data
    :param newest_result: newest data to store
    :return: no return
    """
    # newest_result = PD.DataFrame(newest_result)
    if len(newest_result) == 0:
        return
    my_connection, my_cursor = connect_sql(database)
    used_columns = newest_result.columns
    valid_count = 0

    if drop:
        my_cursor.execute(f'DELETE from {table_name}')
    for record_index in range(len(newest_result)):
        single_record = [newest_result.loc[record_index, column] for column in used_columns]
        sql_sentence = f'INSERT INTO {table_name} ' + str(tuple(used_columns)) \
            .replace("'", '') + ' VALUES ' + str(
            tuple(single_record)).replace("nan", "(%s)" % 'NULL').replace("[", "ARRAY[")
        try:
            my_cursor.execute(sql_sentence)
            my_connection.commit()
            valid_count += 1
            print(f'Uploading {valid_count} records')
        except Exception as e:
            print("Error when writing to SQL", e)
            break
    disconnect_sql(my_connection, my_cursor)
    print(f'*** Upload {valid_count} records into {table_name}')

def remove_sql_records(database, table_name, todo_result, drop=False) -> None:
    """
    remove records from sql
    :param drop:
    :param table_name: which table to locate the data
    :param newest_result: newest data to store
    :return: no return
    """
    # newest_result = PD.DataFrame(newest_result)
    if len(todo_result) == 0:
        return
    my_connection, my_cursor = connect_sql(database)
    used_columns = todo_result.columns
    valid_count = 0

    if drop:
        my_cursor.execute(f'DELETE from {table_name}')

    for record_index in range(len(todo_result)):
        single_record = [todo_result.loc[record_index, column] for column in used_columns]
        single_project = single_record[0]
        single_token_id = single_record[1]
        single_address = single_record[2]
        single_price = single_record[4]
        single_sender = single_record[5]
        single_dest = single_record[6]
        sql_sentence = f'DELETE FROM {table_name} WHERE \
            project=\'{single_project}\' AND\
            token_id=\'{single_token_id}\' AND\
            address=\'{single_address}\' AND\
            price=\'{single_price}\' AND\
            sender=\'{single_sender}\' AND\
            dest=\'{single_dest}\';'
        print(sql_sentence)
        try:
            my_cursor.execute(sql_sentence)
            my_connection.commit()
            valid_count += 1
        except Exception as e:
            print("Error when writing to SQL", e)
            break
    disconnect_sql(my_connection, my_cursor)
    print(f'*** Remove {valid_count} records from {table_name}')

def fetch_data(database, table, columns_info=None, start=None,
    end=None, do_print=False):
    """
    get data from stable between certain time
    :param columns_info:
    :param table: which data table to query
    :param start: from which time to begin the query
    :param end: from which time to end the query
    :param do_print: whether to print state or not
    :return: fetched data
    """
    # database, table, do_print = 'nft_info_test', 'nft_intro', False
    my_connection, my_cursor = connect_sql(database, do_print)
    if not start is None:
        start_str = str(start)
        end_str = str(end)
        sql_sentence = (f"SELECT * FROM {table} where block_time>" +
                   f"='{start_str}' and block_time<='{end_str}';")
    else:
        sql_sentence = f"SELECT * FROM {table};"

    if columns_info is None:
        query_column_name_sql = (f"select column_name from " +
            f"information_schema.columns where table_schema='public'" +
            f"and table_name='{table}';")
        my_cursor.execute(query_column_name_sql)
        columns_info = my_cursor.fetchall()
        columns_info = [_[0] for _ in columns_info]
    format_columns = columns_info

    my_cursor.execute(sql_sentence)
    total_result = my_cursor.fetchall()


    format_values = [[] for _ in format_columns]
    for single_record in total_result:
        # single_record = total_result[0]
        for record_index in range(len(single_record)):
            format_values[record_index].append(single_record[record_index])

    data_dict = {}
    for key_index in range(len(format_columns)):
        data_dict.update({
            format_columns[key_index]: format_values[key_index]
        })

    data_frame = PD.DataFrame(data_dict)

    final_result = data_frame.drop_duplicates(columns_info).reset_index(drop=True)

    disconnect_sql(my_connection, my_cursor, do_print)

    return final_result

def fetch_info_from_table(database, table_name, sql_condition=None) -> list:
    """
    fetch_info_from_table
    :param table_name: the name you want
    :param sql_condition: whether there is detail you need
    :return:
    """
    my_connection, my_cursor = connect_sql(database)
    query_column_name_sql = f"select column_name from information_schema.columns " \
                            f"where table_schema='public' and " \
                            f"table_name='{table_name}';"
    my_cursor.execute(query_column_name_sql)

    if sql_condition is None:
        sql_condition = f"SELECT * FROM {table_name};"
    final_result = PD.read_sql(sql_condition,my_connection)

    return list(PD.DataFrame(final_result).values)

def edit_owner(database, nft_address, token_id, new_owner):
    # database, nft_address, token_id = 'nft_info_test', '0x1d788a3d8133f79a7d8cf2517c4b3c00c8dbbf82', 2
    try:
        my_connection, my_cursor = connect_sql(database)
        my_cursor.execute(f"SELECT * from nft_owner where nft_address = '{nft_address}' and token_id = {token_id};")
        result = my_cursor.fetchall()
        if result:
            my_cursor.execute(
                f"UPDATE nft_owner SET owner='{new_owner}' where nft_address = '{nft_address}' and token_id = {token_id};")
            print('Update successfully')
        else:
            my_cursor.execute(
                f"INSERT INTO nft_owner VALUES('{nft_address}', '{token_id}','{new_owner}');")
            print('Insert successfully')
        my_connection.commit()
        disconnect_sql(my_connection, my_cursor)
        return True
    except:
        print('Fail to update')
        return False

def query_latest_block(database, table_name, network='rinkeby'):
    '''
    query latest block number from certain SQL table
    :param table_name: which table to query
    :return: latest number, CT.INITIAL_BLOCK by default
    '''
    # table_name = 'trader_joe_swap'
    my_connection, my_cursor = connect_sql(database)
    my_cursor.execute(f'SELECT  * FROM {table_name} ORDER BY BLOCK_NUMBER desc LIMIT 1')
    try:
        latest_info = [my_cursor.fetchall()][0][0]
        latest_block = latest_info[0]
    except:
        latest_block = CT.INITIAL_BLOCK[network]
    disconnect_sql(my_connection, my_cursor)
    return latest_block

if __name__ == '__main__':
    # build_database(CT.DATABASE)
    my_connection, my_cursor = connect_sql('nft_info' + CT.DATABASE_SUFFIX, do_print=True)
    disconnect_sql(my_connection, my_cursor, True)