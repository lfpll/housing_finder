import logging
import os
import psycopg2
from bs4 import BeautifulSoup
import requests
import pandas as pd

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
    if soup.find('title').text == 'Error 500' or soup.find("p", string="Anúncio finalizado"):
        return False
    return True


def get_conn(database: str, username: str, password: str, ip: str):
    """ Return a SQL connection in a cursor

    Arguments:
        database {str} -- [name of the database]
        username {str} -- [username use as connection]
        password {str} -- [password for the SQL connection]
        ip {str} -- [ip of database]

    Returns:
        [iter] -- [psycopg2 cursor with the information passed]
    """
    conn = psycopg2.connect(host=ip, database=database,
                            user=username, password=password)
    return conn


def get_urls_with_id(cursor, table_name: str, urls_column_name: str):
    """ Return the urls of the table with their indexes

    Arguments:
        cursor {psycopg2.cursor} -- [cursor of database connection]
        table_name {str} -- [name of the table to insert the offline urls]
        urls_column_name {str} -- [name of the urls column]

    Returns:
        [iter] -- [iterator with all the urls and index of tables]
    """
    query = 'SELECT index,{column} from {table} '.format(
        column=urls_column_name, table=table_name)
    cursor.execute(query)
    return (url[1] for url in cursor.fetchall())


def get_offline_urls(online_function, url_iter: iter, size: int = 50):
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
    for url in url_iter:
        logging.info("checking %s" % url)
        if not url:
            logger.info("No Urls anymore")
            break
        if not online_function(url):
            offline_urls.append(url)
            url_size += 1
    return offline_urls


if __name__ == "__main__":
    USER = os.environ["USER"]
    PWD = os.environ["SQL_PASSWORD"]
    IP = os.environ["IP"]
    TABLE_NAME = os.environ["TABLE_NAME"]
    DATABASE = os.environ["DATABASE"]   

    # Getting all the urls in a iterator
    CONN = get_conn(database=DATABASE, username=USER, password=PWD, ip=IP)
    URLS_ITERATOR = get_urls_with_id(
        CONN.cursor(), table_name=TABLE_NAME, urls_column_name='page_url')

    # Creating the temporary table for the offline urls
    TEMP_TABLE_QUERY = "DROP TABLE IF EXISTS TMP_OFFLINE_URLS; CREATE TABLE TMP_OFFLINE_URLS(id SERIAL PRIMARY KEY, url TEXT NOT NULL);"
    CURSOR_DELETE = CONN.cursor()
    CURSOR_DELETE.execute(TEMP_TABLE_QUERY)
    CONN.commit()

    # Iterating through urls and checking offline
    # TODO replace with async to microservice mock user
    offline_urls = get_offline_urls(online_function=is_url_online_imoveisweb, url_iter=URLS_ITERATOR)
    logger.info("Ingesting {0} offline urls").format(str(len(offline_urls)))
    urls_string = '\'),( \''.join(offline_urls)
    insert_offline_urls_query = "INSERT INTO TMP_OFFLINE_URLS(url) VALUES (\'%s\');" % (
        urls_string)
    CURSOR_DELETE.execute(insert_offline_urls_query)
    CONN.commit()
