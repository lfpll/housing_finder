from bs4 import BeautifulSoup
from google.cloud import pubsub_v1,error_reporting
import google.cloud.logging
import requests
import base64
import os
from datetime import datetime

# Instantiating log client
log_client = google.cloud.logging.Client()
log_client(log_level=logging.INFO, excluded_loggers=("werkzeug",))


# Variables to make pagination works
_IN_BUCKET = os.environ["DELIVER_BUCKET"]
_THIS_FUNCTION_TOPIC = os.environ["THIS_TOPIC"] # 'projects/educare-226818/topics/child_scrape'
_DOWNLOAD_HTML_TOPIC = os.environ["DOWNLOAD_HTML_TOPIC"] # 'projects/educare-226818/topics/html_path'
_PROJECT_NAME = os.environ['PROJECT_NAME'] # educare
_BASE_URL = os.environ['BASE_URL'] # father_url = "https://www.imovelweb.com.br"
_PAGINATION_CSS_SELECTOR = os.environ['PAGINATION_SELECTOR'] # 'li.pag-go-next')
_CSS_SELECTOR = os.environ['PARSE_SELECTOR'] # 'a.go-to-posting


def parse_and_paginate(data, context):
    """Function that parses the page from pagination and sends the next page to pubsub
    
    Arguments:
        data {[base64.encoded]} -- object of pubsub with hashed url on data['data']
        context {[type]} -- context of the pubsub element
    
    Raises:
        Exception: [description]
    """
    log_client = google.cloud.logging.Client()
    log_client(log_level=logging.INFO, excluded_loggers=("werkzeug",))
    json_decoded = json.loads(base64.b64decode(data['data']).decode('utf-8'))
    
    url_decode = json_decoded['url']
    tries = int(json_decoded['tries']) + 1

    # Creating element and getting response
    publisher = pubsub_v1.PublisherClient()


    # Object for failture
    pub_obj_encoded = json.dumps({'url':url_decode,'tries':tries}).encode("utf-8")
    
    response = requests.get(url_decode)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content,'lxml')
        next_url = soup.select(_PAGINATION_CSS_SELECTOR)
        # If the request fail send again to pubsub
        if soup.select('title')[0].text == 'Error 500':
            if tries < 5:
                publisher.publish(_THIS_FUNCTION_TOPIC, pub_obj_encoded)
            else:
                logging.warn("{current_url} Was already parsed 5 times, ended with 500 page".format(current_url=url))
            quit()
        elif len(next_url) <= 0:
            # Loging if there is no next url
            date = str(datetime.now())
            logging.info("Last url of {date} is {url}".format(date=date,current_url=url))
        else:
            next_url = next_url[0].select('a')[0]['href']
            publisher.publish(_THIS_FUNCTION_TOPIC, (_BASE_URL + next_url).encode('utf-8'))
        
        # Products <a/> attributes to be parsed
        products_soups = soup.select(_CSS_SELECTOR)
        if len(products_soups) <= 0:
            raise Exception('Invalid value of products')
        products_url = [_BASE_URL + attribute['href'] for attribute in products_soups]

        # Publishing urls to the products topic
        for product in products_url:
            publisher.publish(_DOWNLOAD_HTML_TOPIC, product.encode('utf-8'))
    else:
        if tries < 5:
            publisher.publish(_THIS_FUNCTION_TOPIC, pub_obj_encoded)
        else:
            logging.warn("{current_url} Was already parsed 5 times, ended with http errors".format(current_url=url))