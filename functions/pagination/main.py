import json
import base64
import os
from datetime import datetime
import logging
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1, logging as cloud_logging
import requests

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0'}

# Instantiating log client
LOG_CLIENT = cloud_logging.Client()
handler = LOG_CLIENT.get_default_handler()
LOGGER = logging.getLogger('cloudLogger')
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(handler)

# Variables to make pagination works
# 'projects/educare-226818/topics/child_scrape'
_THIS_FUNCTION_TOPIC = os.environ["THIS_TOPIC"]
# 'projects/educare-226818/topics/html_path'
_DOWNLOAD_HTML_TOPIC = os.environ["DOWNLOAD_HTML_TOPIC"]
# father_url = "https://www.imovelweb.com.br"
_BASE_URL = os.environ['BASE_URL']
# 'li.pag-go-next'
_PAGINATION_CSS_SELECTOR = os.environ['PAGINATION_SELECTOR']
_CSS_SELECTOR = os.environ['PARSE_SELECTOR']  # 'a.go-to-posting


def parse_and_paginate(message, context):
    """Function that parses the page from pagination and sends the next page to pubsub

    Arguments:
        data {[base64.encoded]} -- object of pubsub with hashed url on data['data']
        context {[type]} -- context of the pubsub element

    Raises:
        Exception: [Error, page nas no places to rent]
    """

    def __error_path(publisher, pub_obj_encoded, tries, url, error):
        """Function to handle possible errors on pagination

        Args:
            pub_obj_encoded ([dict]): [pubsub dict witn infos of the page and tries]
            tries ([int]): [number of tries that this page was tried]
            url ([str]): [url to be parsed for pagination]
        """
        if tries < 5:
            publisher.publish(_THIS_FUNCTION_TOPIC, pub_obj_encoded)
        else:
            logging.error(
                "%s pagination already parsed 5 times, ended with %s page", url, error)
    
    data = base64.b64decode(message['data']).decode('utf-8')
    json_decoded = json.loads(data)
    url_decode = json_decoded['url']

    # Adding number o tries
    tries = 0
    if 'tries' in json_decoded:
        tries = int(json_decoded['tries']) + 1

    # Creating element and getting response
    publisher = pubsub_v1.PublisherClient()

    # Object for failture
    pub_obj_encoded = json.dumps(
        {'url': url_decode, 'tries': tries}).encode("utf-8")

    response = requests.get(url_decode,headers=HEADERS)
    if response.status_code == 200:

        soup = BeautifulSoup(response.content, 'lxml')


        # If the request has error page 500 follow error path
        if soup.select('title')[0].text == 'Error 500':
            __error_path(publisher, pub_obj_encoded,
                         tries, url_decode, error=500)
        else:
            # Next url soup object
            next_url = soup.select(_PAGINATION_CSS_SELECTOR)
            if next_url:
                # Loging if there is no next url and publish
                next_url = next_url[0].select('a')[0]['href']
                pub_next_obj = json.dumps({"url":_BASE_URL + next_url})
                publisher.publish(_THIS_FUNCTION_TOPIC,pub_next_obj.encode('utf-8'))
            else:
                logging.info("Last url %s", url_decode)

            # Products <a/> attributes to be parsed
            products_soups = soup.select(_CSS_SELECTOR)
            if not products_soups:
                raise Exception('Invalid value of products')
            products_url = [_BASE_URL + attribute['href']
                            for attribute in products_soups]

            # Publishing urls to the products topic
            for product in products_url:
                product_obj = json.dumps({"url":product})
                publisher.publish(_DOWNLOAD_HTML_TOPIC, product_obj.encode('utf-8'))
    else:
        __error_path(publisher, pub_obj_encoded, tries,
                     url_decode, error=response.status_code)
