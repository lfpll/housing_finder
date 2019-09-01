import os

os.environ["DELIVER_BUCKET"] = 'deliver_bucket'
os.environ["THIS_TOPIC"] = 'projects/educare-226818/topics/child_scrape'
os.environ["DOWNLOAD_HTML_TOPIC"] = 'projects/educare-226818/topics/html_path'
os.environ['PROJECT_NAME'] = 'educare'
os.environ['BASE_URL'] = "https://www.imovelweb.com.br"
os.environ['PAGINATION_SELECTOR'] = 'li.pag-go-next'
os.environ['PARSE_SELECTOR'] = 'a.go-to-posting'

from google.cloud import pubsub_v1
from google.auth import credentials
from functions.pagination import main as paging
import base64
import json



def test_pagination():
    data_path = 'pagination_page.html'
    encoded_obj = base64.b64encode(json.dumps({'data':{'url':data_path}}).encode('utf-8'))
    paging.parse_and_paginate(encoded_obj,'')
    breakpoint()
