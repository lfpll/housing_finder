import pytest
import os
import logging
from google.cloud import pubsub_v1
import requests

os.environ['OUT_TOPIC'] = 'out_topic'


@pytest.fixture
def mock_gcloud_pubsub(monkeypatch):
    class pub_substitute:
        def __init__(self):
            logging.info("PubSub initializaed")

        def publish(self,out_topic,data):
            logging.info("{data} written to {topic}".format(data,out_topic))


    monkeypatch.setattr(pubsub_v1,'PublisherClient',pub_substitute)


@pytest.fixture
def mock_requets(monkeypatch):
    def get_200_replacer(file_path):
        return open(file_path,'r').read()
    monkeypatch.setattr(requests,'get',get_200_replacer)