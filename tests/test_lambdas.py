import os
from google.cloud import pubsub_v1
import pytest
import requests
import base64
import json
from unittest.mock import MagicMock
from unittest import mock

os.environ["DELIVER_BUCKET"] = 'deliver_bucket'
os.environ["THIS_TOPIC"] = 'projects/educare-226818/topics/child_scrape'
os.environ["DOWNLOAD_HTML_TOPIC"] = 'projects/educare-226818/topics/html_path'
os.environ["BASE_URL"] = "http://www.imovelweb.com.br"
os.environ['PAGINATION_SELECTOR'] = 'li.pag-go-next'
os.environ['PARSE_SELECTOR']  = 'a.go-to-posting'


from pull_data.pagination import main as paging
@pytest.mark.test_lambdas
class Test_Pagination:
    # Variables used to set the functions parameters
    
    @pytest.fixture(autouse=True)
    def mock_200_requests(self,monkeypatch):
        def replace_200_http(file_path,**args):
                response_mock = requests.Response()
                response_mock.status_code = 200
                response_mock._content = open(file_path)
                return response_mock
        monkeypatch.setattr(requests, 'get', replace_200_http)

    def test_pagination_online_page(self,sample_folder,mock_cloud_logging,mock_cloud_error_reporting,mock_cloud_pubsub_v1):
        # Tests scraping pagination for correct page
        data_path = '{sample_folder}sample_pagination/pagination_page.html'.format(sample_folder=sample_folder)
        encoded_obj = {'data':base64.b64encode(json.dumps({'url':data_path}).encode('utf-8'))}
        paging.parse_and_paginate(message=encoded_obj,context='')
        import pdb;pdb.set_trace()

    def test_pagination_offline_page(self,sample_folder):
        pass

    def test_pagination_500_page(self,sample_folder):        
        pass

    def test_pagination_max_tries(self,sample_folder):
        pass