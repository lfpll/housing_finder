from pull_data.pagination import main as paging
from pull_data.download_html import main as download_html
from unittest import mock
import pandas as pd
from unittest.mock import MagicMock
import json
import base64
from io import BytesIO
import requests

import pytest
from google.cloud import pubsub_v1
import os
from functools import partial
import shutil


def submit_message_paging(page_path, tries=None):
    # Expected format of input
    data = {'url': page_path}
    if tries is not None:
        data['tries'] = tries

    encoded_obj = {'data': base64.b64encode(
        json.dumps(data).encode('utf-8'))}
    # Capturing the output using Mocked pubsub
    paging.parse_and_paginate(message=encoded_obj, context='')


def submit_message_download(page_path):
    # Expected format of input
    data = {'url': page_path}
    encoded_obj = {'data': base64.b64encode(
        json.dumps(data).encode('utf-8'))}
    # Capturing the output using Mocked pubsub
    download_html.download_page(message=encoded_obj, context='')


@pytest.fixture()
def mock_200_requests(monkeypatch):
    def mock_request(html_path, *args, **kwargs):
        response_mock = requests.Response()
        response_mock.status_code = 200
        response_mock._content = bytes(open(html_path).read().encode("utf-8"))
        return response_mock
    monkeypatch.setattr(requests, 'get', mock_request)


@pytest.mark.test_lambdas
class Test_Pagination:

    # Variables used to set the functions parameters
    os.environ["THIS_TOPIC"] = "mock_this_topic"
    os.environ["DOWNLOAD_HTML_TOPIC"] = "mock_download_topic"
    os.environ["BASE_URL"] = "http://www.imovelweb.com.br"
    os.environ['PAGINATION_SELECTOR'] = 'li.pag-go-next'
    os.environ['PARSE_SELECTOR'] = 'a.go-to-posting'

    def test_pagination_online_page(self, capsys, mock_200_requests, sample_folder,
                                    mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        sample_page_path = '{0}sample_pagination/pagination_page.html'.format(
            sample_folder)
        # Data that is expected from this process
        expected_df = pd.read_csv(
            '{0}pagination_expect_df.csv'.format(sample_folder))

        submit_message_paging(sample_page_path)
        out, err = capsys.readouterr()

        # Creating comparable dataframe from captured mocked pubsub
        rows = [row.split("|") for row in out.strip().split("\n")]
        df_events = pd.DataFrame(
            rows, columns=["event", "topic", "data", "message"]).drop(columns="message")
        pd.testing.assert_frame_equal(df_events, expected_df)

    def test_pagination_500_page(self, mock_200_requests, sample_folder, monkeypatch,
                                 mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        sample_offline_page = '{0}error_500.html'.format(sample_folder)
        # Capturing the output using Mocked pubsub
        with pytest.raises(Exception):
            submit_message_paging(sample_offline_page)
        pass

    def test_pagination_404(self, mock_200_requests, monkeypatch, sample_folder,
                            mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        def mock_404(html_path, *args, **kwargs):
            response_mock = requests.Response()
            response_mock.status_code = 404
            return response_mock
        monkeypatch.setattr(requests, 'get', mock_404)
        # Capturing the output using Mocked pubsub
        with pytest.raises(Exception):
            submit_message_paging("")

    def test_pagination_403(self, capsys, monkeypatch, sample_folder,
                            mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        def mock_403(html_path, *args, **kwargs):
            response_mock = requests.Response()
            response_mock.status_code = 403
            return response_mock
        monkeypatch.setattr(requests, 'get', mock_403)

        submit_message_paging("")
        out, err = capsys.readouterr()
        assert out.strip(
        ) == "PUBSUB|mock_this_topic|{\"url\": \"\", \"tries\": 0}|"

    def test_pagination_max_tries(self, sample_folder, monkeypatch, mock_200_requests,
                                  capsys, mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        def mock_403(html_path, *args, **kwargs):
            response_mock = requests.Response()
            response_mock.status_code = 403
            return response_mock
        monkeypatch.setattr(requests, 'get', mock_403)
        with pytest.raises(requests.exceptions.ConnectionError):
            submit_message_paging('', tries=6)


class Test_pull_data:

    os.environ["DELIVER_BUCKET"] = 'deliver_bucket'
    os.environ["JSON_BUCKET"] = 'json_bucket'
    os.environ["THIS_TOPIC"] = 'mock_this_topic'
    os.environ["PARSE_TOPIC"] = 'mock_parse-topic'

    def test_pull_data_200(self, sample_folder, capsys, mock_200_requests, mock_storage_client,
                           mock_cloud_pubsub_v1, mock_cloud_logging, mock_cloud_error_reporting):
        sample_page_path = '{0}sample_pagination/pagination_page.html'.format(
            sample_folder)
        submit_message_download(sample_page_path)
        out, err = capsys.readouterr()

        msg_exp_new_blob = 'PUBSUB|mock_parse-topic|{"file_path": "pagination_page_html", "url": "/home/luizlobo/Documents/code/imoveis/tests/samples/sample_pagination/pagination_page.html", "new_blob": true}|'
        assert out.strip() == msg_exp_new_blob
        sample_page_path = '{0}sample_pagination/pagination_page.html'.format(
            sample_folder)

        submit_message_download(sample_page_path)
        out, err = capsys.readouterr()
        # Test with existing blob
        msg_exp_update_blob = 'PUBSUB|mock_parse-topic|{"file_path": "pagination_page_html", "url": "/home/luizlobo/Documents/code/imoveis/tests/samples/sample_pagination/pagination_page.html", "new_blob": false}|'
        assert out.strip() == msg_exp_update_blob
        # removing mock blob
        shutil.rmtree(os.environ["TMP_FOLDER"])


    def test_existent_blob(self, mock_storage_client, mock_200_requests):

        pass
