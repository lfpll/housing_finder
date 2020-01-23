import sys
import os
from os.path import dirname
import requests
import pytest
from unittest.mock import MagicMock
from mock_dbs import Mock_Sql_conn,Mock_Client_BigQuery, Mock_storage_client
from google.cloud import pubsub_v1, logging, storage
from unittest.mock import patch
from google.cloud import client, bigquery
from mock_gcloud import *
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
def mock_bigquery_client(monkeypatch, sample_folder):
    mock_sql = Mock_Sql_conn(sample_folder+'mock_data.db')
    mock_sql.load_mock_data_parquet(sample_folder+'mock_rental_data.parquet')
    mock_bq = Mock_Client_BigQuery(mock_sql)
    
    def init(*args):
        return None

    mock_client = MagicMock()
    mock_table = MagicMock()
    mock_table.table.return_value = None
    mock_client.return_value = mock_table

    # Moking bigquery 
    monkeypatch.setattr(bigquery.Client,'__init__',init)
    monkeypatch.setattr(bigquery.Client,'dataset',mock_client)
    monkeypatch.setattr(bigquery.Client,'list_rows',mock_bq.list_rows)
    monkeypatch.setattr(bigquery.Client,'get_table',mock_bq.get_table)
    monkeypatch.setattr(storage,'Client',Mock_storage_client)

@pytest.fixture()
def mock_storage_client(monkeypatch):
    monkeypatch.setattr(storage,'Client',Mock_storage_client)

