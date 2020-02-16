import pandas as pd
import requests
import pytest
import os
import shutil
import json
from datetime import datetime   
from data_mainetance.check_offline_urls import Check_Live_Urls
from tests.mock_gcloud import mock_bigquery_client,mock_storage_client


# This is a test that removes a function from a sql lite like database based on a url column that is offlinee
# Mock database with an url
# Create a SQL lite database
# Populate with fake data
# Create a function that maps the bad urls to requests

# Mock database deletion
@pytest.mark.test_mainetance
class TestClass:

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
