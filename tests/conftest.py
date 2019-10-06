import sys
import os
from os.path import dirname
import requests
import pytest
from google.cloud import pubsub_v1, logging
from mock_dbs import Mock_Sql_conn,Mock_Client_BigQuery
from unittest.mock import patch
from google.cloud import bigquery

PARENT_PATH = os.path.abspath(os.path.join(dirname(__file__), os.pardir))
SAMPLES_FOLDER = PARENT_PATH + '/tests/samples/'
sys.path.append(PARENT_PATH)

@pytest.fixture()
def current_folder():
    return PARENT_PATH

@pytest.fixture()
def sample_folder():
    return SAMPLES_FOLDER
    
@pytest.fixture(autouse=True)
def mock_cloud_logging(monkeypatch):

    class Mock_log_client:

        def __init__(self):
            self.a = ''

        def get_default_handler(self):
            return ''

        def setup_logging(self, log_level, excluded_loggers):
            return ''

    monkeypatch.setattr(logging, 'Client', Mock_log_client)


@pytest.fixture(autouse=True)
def mock_gcloud_publisher(monkeypatch):
    class Mock_pub_client:

        def __init__(self):
            self.a = ''

        def publish(self, topic, message):
            print(topic, message)
    monkeypatch.setattr(pubsub_v1, 'PublisherClient', Mock_pub_client)


@pytest.fixture()
def mock_request_200(monkeypatch,sample_folder):

    def get_200_replacer(file_path):
        response_mock = requests.Response()
        response_mock.status_code = 200
        response_mock._content = open(sample_folder+file_path, 'r').read()
        return response_mock
    monkeypatch.setattr(requests, 'get', get_200_replacer)

@pytest.mark.bq_test
@pytest.fixture()
def mock_bigquery_client(monkeypatch,sample_folder):
    mock_sql = Mock_Sql_conn(sample_folder+'mock_data.db')
    mock_sql.load_mock_data_parquet(sample_folder+'mock_rental_data.parquet')
    mock_class = Mock_Client_BigQuery(mock_sql)
    init = lambda self:  None
    monkeypatch.delattr(bigquery.Client,'__init__',init)
    monkeypatch.setattr(bigquery.Client,'list_rows',mock_class.list_rows)
    monkeypatch.setattr(bigquery.Client,'get_table',mock_class.get_table)

