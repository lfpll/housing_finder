import sqlalchemy
import psycopg2
import os
from bs4 import BeautifulSoup

def url_exists_imoveisweb(response):
    """A function that receives a requests.response object and 
      checks if the url is valid for the imoveis.web website

    Args:
        response ([type]): [description]
    """
    if response.status_code != 200:
        return False
    # Special case of bad status_code system
    soup = BeautifulSoup(response.content, 'lxml')
    if soup.find('title').text == 'Error 500' or soup.find("p",string="Anúncio finalizado"):
        return False
    return True


def get_cursor(server:str,database:str,username:str,password:str,ip:str):
    conn = psycopg2.connect(ip=ip,server=server,database=database,user=username,password=password)
    return conn.cursor()

def get_urls_with_id(cursor,table_name:str,urls_column_name:str):
    query = 'SELECT id,{column} from {table}'.format(column=urls_column_name,table=table_name)
    response = cursor.execute(query)
    return response

if __name__ == "__main__":
    USER = os.environ["USER"]
    PWD = os.environ["PASSWORD"]
    IP = os.environ["IP"]
    TABLE_NAME = os.environ["TABLE_NAME"]
    SERVER = os.environ["SERVER"]
    DATABASE = os.environ["DATABASE"]
    cursor = get_cursor(server=SERVER,database=DATABASE,username=USER,password=PWD,ip=IP)
    urls_list = get_urls_with_id(cursor,table_name=TABLE_NAME,urls_column_name='url')