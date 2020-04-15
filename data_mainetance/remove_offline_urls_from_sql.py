import sqlalchemy
import psycopg2
import logging
import os
from bs4 import BeautifulSoup
import requests
from time import sleep

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def is_url_online_imoveisweb(url):
    """A function that receives an url and 
      checks if the url is valid for the imoveisweb website

    Args:
        response ([type]): [description]
    """
    
    response = requests.get(url)
    logging.info(url)
    logging.info(response.status_code)
    if response.status_code == 404:
        return False
    # Special case of bad status_code system
    soup = BeautifulSoup(response.content, 'lxml')
    if soup.find('title').text == 'Error 500' or soup.find("p",string="Anúncio finalizado"):
        return False
    return True


def get_conn(database:str,username:str,password:str,ip:str):
    """ Return a SQL connection in a cursor
    
    Arguments:
        database {str} -- [name of the database]
        username {str} -- [username use as connection]
        password {str} -- [password for the SQL connection]
        ip {str} -- [ip of database]
    
    Returns:
        [iter] -- [psycopg2 cursor with the information passed]
    """    
    conn = psycopg2.connect(host=ip,database=database,user=username,password=password)
    return conn

def get_urls_with_id(cursor,table_name:str,urls_column_name:str):
    """ Return the urls of the table with their indexes
    
    Arguments:
        cursor {psycopg2.cursor} -- [cursor of database connection]
        table_name {str} -- [name of the table to insert the offline urls]
        urls_column_name {str} -- [name of the urls column]
    
    Returns:
        [iter] -- [iterator with all the urls and index of tables]
    """    
    query = 'SELECT index,{column} from {table} '.format(column=urls_column_name,table=table_name)
    cursor.execute(query)
    return (url[1] for url in cursor.fetchall())


def get_offline_urls(online_function,url_iter:iter,sleep_time:int = 2,size:int = 20):
    """ Check the iterator for offline urls based on the size
    
    Arguments:
        online_function {function} -- [function that checks if the urls is offline]
        url_iter {iter} -- [iterator with the urls]
    
    Keyword Arguments:
        sleep_time {int} -- [time to wait between two urls] (default: {2})
        size {int} -- [numbers of urls to be checked] (default: {50})
    
    Returns:
        [list] -- [a list with all the offline urls]
    """    
    offline_urls = []
    print('ae')
    url_size = 0
    while url_size < size:
        url = next(url_iter,False)
        logging.info("checking %s"%url)
        if not url:
            logger.info("No Urls anymore")
            break
        if not online_function(url):
            offline_urls.append(url)
            url_size += 1
    return offline_urls


if __name__ == "__main__":
    USER = os.environ["USER"]
    PWD = os.environ["PASSWORD"]
    IP = os.environ["IP"]
    TABLE_NAME = os.environ["TABLE_NAME"]
    OUT_TABLE_NAME = os.environ["OUT_TABLE_NAME"]
    DATABASE = os.environ["DATABASE"]

    # Loading query files into
    insert_offline_query = open('./insert_offline_records.sql').read().replace('\n',' ')
    delete_offline_query = open('./delete_offline_data.sql').read().replace('\n',' ')

    # Getting all the urls in a iterator
    conn = get_conn(database=DATABASE,username=USER,password=PWD,ip=IP)
    urls_iter = get_urls_with_id(conn.cursor(),table_name=TABLE_NAME,urls_column_name='page_url')

    # Creating the temporary table for the offline urls
    create_temp_table = "DROP TABLE IF EXISTS tmp_off_urls; CREATE TABLE tmp_off_urls(id SERIAL PRIMARY KEY, url TEXT NOT NULL);"
    cursor_delete = conn.cursor()
    cursor_delete.execute(create_temp_table)
    conn.commit()

    # Cleaning the offline urls and moving into the offline table
    while next(urls_iter, False):
        logger.info("Checking 20 offline urls")
        offline_urls = get_offline_urls(is_url_online_imoveisweb ,urls_iter,1)
        logger.info("Inserting the 20 urls")
        urls_string = '\'),( \''.join(offline_urls)
        insert_query = "INSERT INTO tmp_off_urls(url) VALUES (\'%s\');" % (urls_string)
        cursor_delete.execute(insert_query)
        cursor_delete.execute(insert_offline_query)
        cursor_delete.execute(delete_offline_query)
        conn.commit()