import sys
sys.path.append('..')

from utils import sql_operations as SO
from utils import constants as CT

def create_buidlcamp_table(database, drop_table=False) -> None:
    ''''
    create Buidlcamp table
    :param drop_table: whether to drop original table
    :return: no return, just create the table
    '''

    my_connection, my_cursor = SO.connect_sql(database)

    table_name = f'buidlcamp_table'
    if drop_table:
        my_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    else:
        # judge if the table already exists
        my_cursor.execute(f"select count(*) from pg_class where relname = '{table_name}';")
        result = my_cursor.fetchall()
        if result[0][0] != 0:
            print(f" ** Table {table_name} already exists")
            return

    sql_sentence = f'''
         CREATE TABLE {table_name}(
            id SERIAL PRIMARY KEY,
            user_name text null,
            user_id text null,
            ip_address text null,
            created_time timestamp null
        );
    '''
    my_cursor.execute(sql_sentence)
    print(f' ** Table {table_name} created successfully')
    SO.disconnect_sql(my_connection, my_cursor)

if __name__ == '__main__':
    drop = True
    database = CT.DATABASE
    create_buidlcamp_table(database, drop)
