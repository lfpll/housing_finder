import sys
import os
from os.path import dirname
import requests
import pytest
from unittest.mock import MagicMock
from mock_dbs import Mock_Sql_conn,Mock_Client_BigQuery, Mock_storage_client
from google.cloud import pubsub_v1, logging, storage, bigquery
from unittest.mock import patch
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


