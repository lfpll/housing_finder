import sys
import os
from os.path import dirname
import requests
import pytest
from google.cloud import pubsub_v1, logging

PARENT_PATH = os.path.abspath(os.path.join(dirname(__file__), os.pardir))
SAMPLES_FOLDER = PARENT_PATH + '/samples/'
sys.path.append(PARENT_PATH)

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


@pytest.fixture(autouse=True)
def mock_request_200(monkeypatch):

    def get_200_replacer(file_path):
        response_mock = requests.Response()
        response_mock.status_code = 200
        response_mock._content = open(SAMPLES_FOLDER+file_path, 'r').read()
        return response_mock
    monkeypatch.setattr(requests, 'get', get_200_replacer)
