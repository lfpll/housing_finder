import os
from google.cloud import bigquery
import requests
from bs4 import BeautifulSoup
import time


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
    if soup.select('title')[0].text == 'Error 500':
        return False
    else:
        return True


class Check_Online_Urls:

    def __init__(self, dataset='newdata', table='rentaldata',
                 function_check_deleted=url_exists_imoveisweb, sleep_time=2):
        # Function that checks if requests.response object was deleted
        self.check_deleted = function_check_deleted
        self.client = bigquery.Client()
        self.dataset = dataset
        self.table = table

        self.sleep_time = sleep_time

        # Passing parameterers to be less side effecty
        self.urls_list = self.get_urls_bigquery(
            dataset=self.dataset, table=self.table)

        # Getting the urls that do not exist
        self.deleted_urls = self.check_deleted(urls_list=self.urls_list,
                                               wvalidation_function=self.check_deleted)

    def get_urls_bigquery(self, dataset, table, url_columns='url'):
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
            'url', 'STRING', 'NULLABLE', None, ())]
        # Return the list of urls from the url colum
        rows_list = self.client.list_rows(table, selected_fields=field_url)
        return rows_list

    def check_not_working_urls(self, urls_list, validation_function):
        """A function that check urls offline based on a function

        Args:
            urls_list ([type]): [description]

        Returns:
            [type]: [description]
        """
        delete_urls = []
        for url in urls_list:
            response = requests.get(url)
            if validation_function(response):
                delete_urls.append(url)
        return delete_urls

    def output_to_json(self, offline_list):
        pass
