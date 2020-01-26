from google.cloud import pubsub_v1, error_reporting,storage,bigquery, logging as cloud_logging
from mock_dbs import Mock_Sql_conn,Mock_Client_BigQuery, Mock_storage_client
from functools import partial
from unittest.mock import MagicMock
import pytest
import sys


def init(*args):
    return None


def mock_lazy(*args, element: str):
    """ Base function for mocking simple stuff on gcloud

    Arguments:
        args{list[str or bytes]} -- [ arguments passed to check stoud]
        element {str} -- [gcloud element to be mocked]
    """
    print("{0}  |  {1}".format(
        element, ' |'.join([argument for argument in args])))


@pytest.fixture()
def mock_cloud_logging(monkeypatch):
    mock_cloud_logging = partial(mock_lazy, element="LOGGING")
    monkeypatch.setattr(cloud_logging.Client, '__init__', init)
    monkeypatch.setattr(cloud_logging.Client,
                        'get_default_handler', init)

# This screw the error handling if it happens
@pytest.fixture()
def mock_cloud_error_reporting(monkeypatch,capsys):
    def raise_error(*args,**kwargs):
        raise Exception("ERROR_REPORTING|Test of error worked")
    monkeypatch.setattr(error_reporting.Client, '__init__', init)
    monkeypatch.setattr(error_reporting.Client,
                        "report_exception", raise_error)


@pytest.fixture()
def mock_cloud_pubsub_v1(monkeypatch):
    def mock_publish(pubsubcontext, topic: str, data: bytes, message: str = ''):
        output = "PUBSUB|{topic}|{data}|{message}".format(topic=topic, data=data.decode("utf-8"), message=message)
        print(output)
    monkeypatch.setattr(pubsub_v1.PublisherClient, '__init__', init)
    monkeypatch.setattr(pubsub_v1.PublisherClient, 'publish', mock_publish)


@pytest.fixture()
def mock_storage_client(monkeypatch,sample_folder):
    monkeypatch.setattr(storage,'Client',Mock_storage_client)


@pytest.fixture()
def mock_bigquery_client(monkeypatch, sample_folder):
    mock_sql = Mock_Sql_conn(sample_folder+'mock_data.db')
    mock_sql.load_mock_data_parquet(sample_folder+'mock_rental_data.csv')
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
