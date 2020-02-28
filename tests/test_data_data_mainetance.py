import pandas as pd
import requests
import pytest
import os
import shutil
import json
import sqlalchemy
from datetime import datetime   
from data_mainetance.check_offline_urls import Check_Live_Urls
from tests.mock_gcloud import mock_bigquery_client,mock_storage_client


@pytest.fixture()
def request_replacer(self, sample_folder, monkeypatch):
    def get_replacer_bigquery(url):
        response_mock = requests.Response()
        if url == 'http://test-fail-url.com':
            response_mock.status_code = 400
        elif url == 'http://test-sucess-url.com':
            response_mock.status_code = 200
            response_mock._content = open(
                sample_folder+'sample_pagination/pagination_page.html')
        elif url == 'http://test-500-url.com':
            response_mock.status_code = 200
            response_mock._content = open(sample_folder+'error_500.html')
        return response_mock

    monkeypatch.setattr(requests, 'get', get_replacer_bigquery)

@pytest.mark.test_mainetance_bigquery
class Test_Bigquery:

    def test_live_urls(self, current_folder,request_replacer,sample_folder, mock_bigquery_client, mock_storage_client):
        # Reading mock data to use to assert
        df = pd.read_csv(sample_folder+'mock_rental_data.csv')
        df_size = len(df)
        error_page = df.loc[df['url'] ==
                            'http://test-500-url.com', 'url'].count()
        error_status_code = df.loc[df['url'] ==
                                   'http://test-fail-url.com', 'url'].count()
        table_name = 'mock'
        dataset = 'test'
        # Instanciating class of removal
        validate_instance = Check_Live_Urls(table_name, dataset)
        all_urls = validate_instance.get_urls_bigquery(url_column='url')
        # Comparing the size of the urls and the dataframe mocked
        assert df_size == len(all_urls)

        # Asserting dead urls is correct
        all_urls = [val[0] for val in all_urls]
        dead_urls = validate_instance.check_not_working_urls(
            urls_list=all_urls)
        assert error_page + error_status_code == len(dead_urls)

        # Checking if storing data is working
        bucket_name = "mock_buck"
        validate_instance.store_data_gcs(dead_urls, bucket_name)
        file_path ="%s/tmp/mock_buck/%s" % (current_folder, str(datetime.now().date()).replace(' ','_'))
        assert os.path.exists(file_path)
        with open(file_path) as json_file:
            assert error_page + \
                error_status_code == len(json.load(json_file)['delete'])
        shutil.rmtree(os.environ["TMP_FOLDER"])


class Test_Sql:

    def test_return_cursor():
        # Just check if it's able to connect to the mock database
        get_cursor(database,table,username,password)
        
        
    def test_select_urls(sample_folder):
        df = pd.read(sample_folder+'mock_rental_data.csv')
        urls = get_urls_from_table(cursor,table_name,url_column_name)
        assert len(urls) == df.shape[0]


    def test_offline_500_unit(sample_folder):
        # Checking if the function which is offline 
        mock_html = open(sample_folder+'error_500.html')
        response_mock = requests.Response()
        response_mock.status_code = 200
        response_mock._content = open(sample_folder+'error_500.html')
        bool_resp = is_offline(response_mock)
        assert bool_resp


    def test_error_http_code():
        response_mock = requests.Response()
        response_mock.status_code = 500
        bool_resp = is_offline(response_mock)
        assert bool_resp
    
    def test_remove_urls():
        urls_list = ['http://test-fail-url.com']
        remove_urls(cursor,table_name,url_column_name,urls_list)
        assert 'http://test-fail-url.com' not in get_urls_from_table(cursor,table_name,url_column_name)


    def test_live_urls(self, current_folder,request_replacer,sample_folder, mock_bigquery_client, mock_storage_client):
        # Reading mock data to use to assert
        df = pd.read_csv(sample_folder+'mock_rental_data.csv')
        df_size = len(df)
        error_page = df.loc[df['url'] ==
                            'http://test-500-url.com', 'url'].count()
        error_status_code = df.loc[df['url'] ==
                                   'http://test-fail-url.com', 'url'].count()
        table_name = 'mock'
        dataset = 'test'
        # Instanciating class of removal
        validate_instance = Check_Live_Urls(table_name, dataset)
        all_urls = validate_instance.get_urls_bigquery(url_column='url')
        # Comparing the size of the urls and the dataframe mocked
        assert df_size == len(all_urls)

        # Asserting dead urls is correct
        all_urls = [val[0] for val in all_urls]
        dead_urls = validate_instance.check_not_working_urls(
            urls_list=all_urls)
        assert error_page + error_status_code == len(dead_urls)

        # Checking if storing data is working
        bucket_name = "mock_buck"
        validate_instance.store_data_gcs(dead_urls, bucket_name)
        file_path ="%s/tmp/mock_buck/%s" % (current_folder, str(datetime.now().date()).replace(' ','_'))
        assert os.path.exists(file_path)
        with open(file_path) as json_file:
            assert error_page + \
                error_status_code == len(json.load(json_file)['delete'])
        shutil.rmtree(os.environ["TMP_FOLDER"])