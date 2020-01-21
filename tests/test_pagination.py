import os
from google.cloud import cloud_logging
from google.cloud import pubsub_v1
from google.auth import credentials
import base64
import json


def mock_logging():
    cloud_logging.Client = MagickMock

def test_pagination(sample_folder):
    # Setting variables that are required
    os.environ["DELIVER_BUCKET"] = 'deliver_bucket'
    os.environ["THIS_TOPIC"] = 'projects/educare-226818/topics/child_scrape'
    os.environ["DOWNLOAD_HTML_TOPIC"] = 'projects/educare-226818/topics/html_path'
    os.environ["BASE_URL"] = "http://www.imovelweb.com.br"
    os.environ['PAGINATION_SELECTOR'] = 'li.pag-go-next'
    os.environ['PARSE_SELECTOR']  = 'a.go-to-posting

    from functions.pagination import main as paging
    
    # Mocking a context with the url
    data_path = '{sample_folder}/pagination/pagination_page.html'.format(sample_folder=sample_folder)
    encoded_obj = base64.b64encode(json.dumps({'data':{'url':data_path}}).encode('utf-8'))
    paging.parse_and_paginate(encoded_obj,'')
    breakpoint()
