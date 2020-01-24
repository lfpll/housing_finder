from datetime import datetime
import time
import json
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery, storage
import argparse


def url_exists_imoveis_web(response):
    """A function that receives a requests.response object and 
      checks if the url is valid for the imoveis web website

    Args:
        response ([type]): [description]
    """
    if response.status_code != 200:
        return False
    # Special case of bad status_code system
    soup = BeautifulSoup(response.content, 'lxml')
    if soup.find('title').text.strip() == 'Error 500' or soup.find("h5",{"class":"mdl-titlea"}).text.strip() == "Que pena!! Este anúncio finalizou.":
        return False
    return True


class Check_Live_Urls:

    def __init__(self, dataset='newdata', table='rentaldata',
                 function_check_deleted=url_exists_imoveis_web, sleep_time=1):
        # Function that checks if requests.response object was deleted
        self.check_deleted = function_check_deleted
        self.client = bigquery.Client()
        self.dataset = dataset
        self.table = table
        self.sleep_time = sleep_time

    def get_urls_bigquery(self, dataset, table, url_column='url'):
        """Function that returns the urls from the column in bigquery

        Args:
            dataset ([type]): [description]
            table ([type]): [description]
            url_columns (str, optional): [description]. Defaults to 'url'.

        Returns:
            [type]: [description]
        """
        table_ref = self.client.dataset(dataset).table(table)
        table = self.client.get_table(table_ref)
        field_url = [bigquery.schema.SchemaField(
            url_column, 'STRING', 'NULLABLE', None, ())]
        # Return the list of urls from the url colum
        rows_list = self.client.list_rows(table, selected_fields=field_url)
        return rows_list

    def check_not_working_urls(self, urls_list, validation_function=None):
        """A function that check urls offline based on a function

        Args:
            urls_list ([type]): [description]

        Returns:
            [type]: [description]
        """
        # Getting the function that checks if the url was deleted
        if validation_function is None:
            validation_function = self.check_deleted
        delete_urls = []

        for url in urls_list:
            response = requests.get(url)
            if not validation_function(response):
                delete_urls.append(url)
        time.sleep(self.sleep_time)
        return delete_urls

    def store_data_gcs(self, offline_list, json_bucket):
        """ 
        
        Arguments:
            offline_list {[list]} -- [list of urls]
            json_bucket {[str]} -- [path to json]
        """        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(json_bucket)
        date_today = str(datetime.now().date()).replace(' ','_')
        blob = bucket.blob('{0}'.format(date_today))
        json_file = json.dumps({'delete': offline_list})
        blob.upload_from_string(json_file)

if __name__== "__main__":
    parser = argparse.ArgumentParser(description='Arguments of table and dataset')
    parser.add_argument('--dataset', type=str,
                    help='Path of output of the dataframe', required=True)
    parser.add_argument('--table', type=str,
                    help='Path of output of the dataframe', required=True)
    bq_args = vars(parser.parse_known_args())
    check_urls = Check_Live_Urls(dataset=bq_args['dataset'],table=bq_args['table'])