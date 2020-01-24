from pull_data.pagination import main as paging
from unittest import mock
import pandas as pd
from unittest.mock import MagicMock
import json
import base64
import requests
import pytest
from google.cloud import pubsub_v1
import os
from functools import partial

@pytest.mark.test_lambdas
class Test_Pagination:

    # Variables used to set the functions parameters
    os.environ["THIS_TOPIC"] = "mock_this_topic"
    os.environ["DOWNLOAD_HTML_TOPIC"] = "mock_download_topic"
    os.environ["BASE_URL"] = "http://www.imovelweb.com.br"
    os.environ['PAGINATION_SELECTOR'] = 'li.pag-go-next'
    os.environ['PARSE_SELECTOR'] = 'a.go-to-posting'

    def mock_request(mock_html,*args, **kwargs,code=200):
        response_mock = requests.Response()
        response_mock.status_code = code
        response_mock._content = open(file_path)
        return response_mock

    @pytest.fixture()
    def mock_200_requests(self, monkeypatch):
        replace_200_http = partial(self.mock_request,code=200)
        monkeypatch.setattr(requests, 'get', replace_200_http)
    
    def submit_message(self,page_path):
        # Expected format of input
        encoded_obj = {'data': base64.b64encode(
            json.dumps({'url': page_path}).encode('utf-8'))}

        # Capturing the output using Mocked pubsub
        paging.parse_and_paginate(message=encoded_obj, context='')


    def test_pagination_online_page(self, capsys, mock_200_requests, sample_folder,mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):

        sample_page_path = '{0}sample_pagination/pagination_page.html'.format(sample_folder)
        # Data that is expected from this process
        expected_df = pd.read_csv('{0}pagination_expect_df.csv'.format(sample_folder))

        self.submit_message(sample_page_path)
        out, err = capsys.readouterr()

        # Creating comparable dataframe from captured mocked pubsub
        rows = [row.split("|") for row in out.strip().split("\n")]
        df_events = pd.DataFrame(
            rows, columns=["event", "topic", "data", "message"]).drop(columns="message")
        pd.testing.assert_frame_equal(df_events, expected_df)


    def test_pagination_offline_page(self, sample_folder,mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        sample_offline_page = '{0}error_500.html'.format(sample_folder)
        
        # Expected format of input
        encoded_obj = {'data': base64.b64encode(
            json.dumps({'url': sample_offline_page}).encode('utf-8'))}

        # Capturing the output using Mocked pubsub
        paging.parse_and_paginate(message=encoded_obj, context='')
        import pdb;pdb.set_trace()

        pass

    def test_pagination_500_page(self, sample_folder):
        pass

    def test_pagination_403_page(self, sample_folder):
        pass

    def test_pagination_max_tries(self, sample_folder):
        pass
