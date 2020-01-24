from google.cloud import pubsub_v1, error_reporting, logging as cloud_logging
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
def mock_cloud_error_reporting(monkeypatch):

    def raise_error():
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
