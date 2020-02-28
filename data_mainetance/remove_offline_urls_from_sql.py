import sqlalchemy
import psycopg2

def get_cursor(server,dbname:str,username:str,password:str):
    conn_params_str = "server={server} dbname={dbname} user={username} password={password}"
    conn = psycopg2.connect(conn_params_str.format(server=server,dbname=dbname,user=username,password=password))
    return conn.cursor()

def get_urls_with_id(cursor,table_name:str,urls_column_name:str):
    query = 'SELECT id,{column} from {table}'.format(column=urls_column_name,table=table_name)
    response = cursor.execute(query)
    return response