from google.cloud import pubsub_v1, error_reporting, logging as cloud_logging
from functools import partial
import pytest

def init(*args):
    return None


def mock_lazy(topic: str, message: bytes, element: str):
    """ Base function for mocking simple stuff on gcloud
    
    Arguments:
        topic {str} -- [description]
        message {bytes} -- [description]
        element {str} -- [description]
    """
    print("{0}  |  {1}  |  {2}".format(
        element, topic, message.decode("utf-8")))

@pytest.fixture()
def mock_cloud_logging(monkeypatch):
    mock_cloud_logging = partial(mock_lazy, element="LOGGING")
    monkeypatch.setattr(cloud_logging.Client, '__init__', init)
    monkeypatch.setattr(cloud_logging.Client,
                        'get_default_handler', init)
        
@pytest.fixture()
def mock_cloud_error_reporting(monkeypatch):
    mock_error_report = partial(mock_lazy, element="ERROR_REPORTING")
    monkeypatch.setattr(error_reporting.Client, '__init__', init)
    monkeypatch.setattr(error_reporting.Client, 'report_exception', mock_error_report)

@pytest.fixture()
def mock_cloud_pubsub_v1(monkeypatch):
    mock_publish = partial(mock_lazy, element="PUBSUB")
    monkeypatch.setattr(pubsub_v1.PublisherClient, '__init__', init)
    monkeypatch.setattr(pubsub_v1.PublisherClient, 'publish', mock_publish)
