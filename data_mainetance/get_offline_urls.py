import logging
import os
import psycopg2
from bs4 import BeautifulSoup
import requests
import pandas as pd
import argparse

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


def get_urls_with_id(cursor, table_name: str, urls_column_name: str):
    """ Return the urls of the table with their indexes

    Arguments:
        cursor {psycopg2.cursor} -- [cursor of database connection]
        table_name {str} -- [name of the table to insert the offline urls]
        urls_column_name {str} -- [name of the urls column]

    Returns:
        [iter] -- [iterator with all the urls and index of tables]
    """
    query = 'SELECT {column} from {table} '.format(
        column=urls_column_name, table=table_name)
    cursor.execute(query)
    return (url[0] for url in cursor.fetchall())



def get_offline_urls(online_function, url_iter: iter):
    """ Check the iterator for offline urls based on the size

    Arguments:
        online_function {function} -- [function that checks if the urls is offline]
        url_iter {iter} -- [iterator with the urls]

    Returns:`
        [list] -- [a list with all the offline urls]
    """
    offline_urls = []
    for url in url_iter:
        logging.info("checking %s" % url)
        if not url:
            logger.info("No Urls anymore")
            break
        if not online_function(url):
            offline_urls.append(url)
    return offline_urls


if __name__ == "__main__":
    USER = os.environ["USER"]
    PWD = os.environ["SQL_PASSWORD"]
    IP = os.environ["IP"]
    TABLE_NAME = os.environ["TABLE_NAME"]
    DATABASE = os.environ["DATABASE"]   

    # Getting all the urls in a iterator
    conn = psycopg2.connect(host=IP, database=DATABASE,
                            user=USER, password=PWD)
    URLS_ITERATOR = get_urls_with_id(
    conn.cursor(), table_name=TABLE_NAME, urls_column_name='page_url')

    # Creating the temporary table for the offline urls
    TEMP_TABLE_QUERY = "DROP TABLE IF EXISTS TMP_OFFLINE_URLS; CREATE TABLE TMP_OFFLINE_URLS(id SERIAL PRIMARY KEY, url TEXT NOT NULL);"
    cursor = conn.cursor()
    cursor.execute(TEMP_TABLE_QUERY)
    conn.commit()

    # Checking if the offline urls are live
    offline_urls = get_offline_urls(online_function=is_url_online_imoveisweb, url_iter=URLS_ITERATOR)
    logger.info("Ingesting {0} offline urls".format(str(len(offline_urls))))
    urls_string = '\'),( \''.join(offline_urls)
    insert_offline_urls_query = "INSERT INTO TMP_OFFLINE_URLS(url) VALUES (\'%s\');" % (
        urls_string)
    cursor.execute(insert_offline_urls_query)
    conn.commit()
