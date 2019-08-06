import pytest
import os
import logging
import base64
import requests
import queue
from google.cloud import pubsub_v1


# Class to substitute the pub sub on python
class pub_substitute:
        def __init__(self):
            pub_test_queue = queue.Queue()
            logging.info("PubSub initialized")

        def publish(self,out_topic,data):
            self.pub_test_queue.put(data)
            data_decoded = base64.b64decode(data)
            logging.info("{data} written to {topic}".format(data_decoded,out_topic))
        
        def subscribe(self,in_topic):
            logging.info("{data} read from {topic}".format(data,out_topic))
            return self.pub_test_queue.get()

# Mocking the /cloud pubsub function
@pytest.fixture(autouse=True)
def mock_gcloud_pubsub(monkeypatch):
    monkeypatch.setattr(pubsub_v1,'PublisherClient',pub_substitute)


@pytest.fixture
def mock_request_200(monkeypatch):
    def get_200_replacer(file_path):
        return {'content':open(file_path,'r').read(),'status_code':200}
    monkeypatch.setattr(requests,'get',get_200_replacer)    