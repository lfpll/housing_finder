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



    @pytest.fixture()
    def mock_200_requests(self, monkeypatch):
        def mock_request(html_path,*args, **kwargs):
            response_mock = requests.Response()
            response_mock.status_code = 200
            response_mock._content = open(html_path)
            return response_mock
        monkeypatch.setattr(requests, 'get', mock_request)

    def submit_message(self,page_path, tries = None):
        # Expected format of input
        data = {'url': page_path}
        if tries is not None:
            data['tries'] = tries

        encoded_obj = {'data': base64.b64encode(
            json.dumps(data).encode('utf-8'))}
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


    def test_pagination_500_page(self, mock_200_requests,sample_folder,monkeypatch,mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        sample_offline_page = '{0}error_500.html'.format(sample_folder)
        # Capturing the output using Mocked pubsub
        with pytest.raises(Exception):
            self.submit_message(sample_offline_page)
        pass

    def test_pagination_404(self, mock_200_requests,monkeypatch, sample_folder,mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        def mock_404(html_path,*args, **kwargs):
            response_mock = requests.Response()
            response_mock.status_code = 404
            return response_mock
        monkeypatch.setattr(requests, 'get', mock_404)
        # Capturing the output using Mocked pubsub
        with pytest.raises(Exception):
            self.submit_message("")

    def test_pagination_403(self, capsys,monkeypatch, sample_folder,mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        def mock_403(html_path,*args, **kwargs):
            response_mock = requests.Response()
            response_mock.status_code = 403
            return response_mock
        monkeypatch.setattr(requests, 'get', mock_403)
        
        self.submit_message("")
        out, err = capsys.readouterr()
        assert out.strip() == "PUBSUB|mock_this_topic|{\"url\": \"\", \"tries\": 0}|"


    def test_pagination_max_tries(self, sample_folder,monkeypatch,mock_200_requests, capsys ,mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        def mock_403(html_path,*args, **kwargs):
            response_mock = requests.Response()
            response_mock.status_code = 403
            return response_mock
        monkeypatch.setattr(requests, 'get', mock_403)
        with pytest.raises(requests.exceptions.ConnectionError):
            self.submit_message('',tries=6)
