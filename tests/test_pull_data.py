from pull_data.pagination import main as paging
from pull_data.download_html import main as download_html
from pull_data.parse_rental import main as parse_rental
from unittest import mock
import pandas as pd
from unittest.mock import MagicMock
import json
import base64
from io import BytesIO
import requests
from tests.mock_gcloud import mock_storage_client, mock_cloud_error_reporting,mock_cloud_logging, mock_cloud_pubsub_v1
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


def submit_message_download(page_path,tries=None):
    # Expected format of input
    data = {'url': page_path}
    if tries is not None:
        data['tries'] = tries
    encoded_obj = {'data': base64.b64encode(
        json.dumps(data).encode('utf-8'))}
    # Capturing the output using Mocked pubsub
    download_html.download_html(message=encoded_obj, context='')
    
@pytest.fixture()
def mock_200_requests(monkeypatch):
    def mock_request(html_path, *args, **kwargs):
        response_mock = requests.Response()
        response_mock.status_code = 200
        response_mock._content = bytes(open(html_path).read().encode("utf-8"))
        return response_mock
    monkeypatch.setattr(requests, 'get', mock_request)


@pytest.mark.test_lambdas
class Test_pagination:

    # Variables used to set the functions parameters
    os.environ["THIS_TOPIC"] = "mock_this_topic"
    os.environ["DOWNLOAD_HTML_TOPIC"] = "mock_download_topic"
    os.environ["BASE_URL"] = "http://www.imovelweb.com.br"
    os.environ['PAGINATION_SELECTOR'] = 'li.pag-go-next'
    os.environ['PARSE_SELECTOR'] = 'a.go-to-posting'

    def test_http_code_200(self, capsys, mock_200_requests, sample_folder,
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

    def test_special_case_500_page(self, mock_200_requests, sample_folder, monkeypatch,
                                 mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        sample_offline_page = '{0}error_500.html'.format(sample_folder)
        # Capturing the output using Mocked pubsub
        with pytest.raises(Exception):
            submit_message_paging(sample_offline_page)
        pass

    def test_http_code_404(self, mock_200_requests, monkeypatch, sample_folder,
                            mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        def mock_404(html_path, *args, **kwargs):
            response_mock = requests.Response()
            response_mock.status_code = 404
            return response_mock
        monkeypatch.setattr(requests, 'get', mock_404)
        # Capturing the output using Mocked pubsub
        with pytest.raises(Exception):
            submit_message_paging("")

    def test_http_code_403(self, capsys, monkeypatch, sample_folder,
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

    def test_max_tries(self, sample_folder, monkeypatch, mock_200_requests,
                                  capsys, mock_cloud_logging, mock_cloud_error_reporting, mock_cloud_pubsub_v1):
        def mock_403(html_path, *args, **kwargs):
            response_mock = requests.Response()
            response_mock.status_code = 403
            return response_mock
        monkeypatch.setattr(requests, 'get', mock_403)
        with pytest.raises(requests.exceptions.ConnectionError):
            submit_message_paging('', tries=6)


class Test_download_html:

    os.environ["OUTPUT_HTML_BUCKET"] = 'deliver_bucket'
    os.environ["THIS_TOPIC"] = 'mock_this_topic'
    os.environ["OUTPUT_JSON_TOPIC"] = 'mock_parse-topic'
    os.environ["DELIVER_BUCKET"] = 'deliver_bucket'
    os.environ["JSON_BUCKET"] = 'json_bucket'
    os.environ["THIS_TOPIC"] = 'mock_this_topic'
    os.environ["PARSE_TOPIC"] = 'mock_parse-topic'

    def test_http_code_200(self, sample_folder, capsys, mock_200_requests, mock_storage_client,
                           mock_cloud_pubsub_v1, mock_cloud_logging, mock_cloud_error_reporting):
        sample_page_path = '{0}sample_pagination/pagination_page.html'.format(
            sample_folder)
        submit_message_download(sample_page_path)
        out, err = capsys.readouterr()

        msg_exp_new_blob = ['PUBSUB', 'mock_parse-topic', '{"file_path": "pagination_page.html", "url": "'+sample_page_path+'\"}', '']
        assert all([a == b for a, b in zip(msg_exp_new_blob,out.strip().split("|"))])
        submit_message_download(sample_page_path)
        out, err = capsys.readouterr()
        shutil.rmtree(os.environ["TMP_FOLDER"])

    def test_max_tries(self, capsys,monkeypatch, mock_200_requests, mock_storage_client,
                           mock_cloud_pubsub_v1, mock_cloud_logging, mock_cloud_error_reporting):
        def mock_403(html_path, *args, **kwargs):
            response_mock = requests.Response()
            response_mock.status_code = 403
            return response_mock
        monkeypatch.setattr(requests, 'get', mock_403)

        submit_message_download("")
        out, err = capsys.readouterr()
        assert out.strip(
        ) == "PUBSUB|mock_this_topic|{\"url\": \"\", \"tries\": 0}|"
        out, err = capsys.readouterr()
        with pytest.raises(Exception):
            submit_message_download("",tries=6)

class Test_parse_rental:
    
    os.environ['HTML_IN_BUCKET']  = "in_bucket" 
    os.environ['JSON_OUT_BUCKET']  = "out_bucket"
    os.environ["OUTPUT_GCS_FOLDER"] = "stage"


    def test_expected_output(self, sample_folder, capsys, mock_storage_client, mock_cloud_error_reporting):
        data = {'url': "normal_page.html","file_path":"normal_page.html","new_blob":True}
        encoded_obj = {'data': base64.b64encode(
            json.dumps(data).encode('utf-8'))}

        # Capturing the output using Mocked pubsub
        in_bucket = os.environ["TMP_FOLDER"] +  os.environ['HTML_IN_BUCKET']
        out_bucket = os.environ["TMP_FOLDER"] + os.environ['JSON_OUT_BUCKET']

        os.makedirs(in_bucket,exist_ok=True)
        os.makedirs(out_bucket+"/stage",exist_ok=True)

        shutil.copy(sample_folder+"sample_download_html/normal_page.html",in_bucket)
        parse_rental.parse_rental(encoded_obj,"")
        processed = json.loads(open(out_bucket+"/%s/normal_page.json"%os.environ["OUTPUT_GCS_FOLDER"]).read())

        not_processed = json.loads(open(sample_folder+"normal_page.json").read())

        del processed["date_stored"]
        del not_processed["date_stored"]
        assert  processed.items() <= not_processed.items()
        out, err = capsys.readouterr()
        shutil.rmtree(os.environ["TMP_FOLDER"])