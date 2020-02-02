import json
import base64
import os
from datetime import datetime
import logging
import requests
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1, error_reporting, logging as cloud_logging
from requests.exceptions import ConnectionError, HTTPError


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0'}

def parse_and_paginate(message, context):
    """Function that parses the page from pagination and sends the next page to pubsub

    Arguments:
        data {[base64.encoded]} -- object of pubsub with hashed url on data['data']
        context {[]} -- context of the pubsub element

    Raises:
        Exception: [Error, page is invalid or has no data]
    """    
   # pubsub topic of this function
    _THIS_FUNCTION_TOPIC = os.environ["THIS_TOPIC"]     
    # pubsub topic that the function will be passed
    _DOWNLOAD_HTML_TOPIC = os.environ["DOWNLOAD_HTML_TOPIC"]     
    # base url to be aggregated to parsed url
    _BASE_URL = os.environ['BASE_URL']     
    # selector of the css paging button
    _PAGINATION_CSS_SELECTOR = os.environ['PAGINATION_SELECTOR']     
    # selector of the parsed objects
    _CHILD_CSS_SELECTOR = os.environ['PARSE_SELECTOR']  

    # Instantiating log client
    LOG_CLIENT = cloud_logging.Client()
    HANDLER = LOG_CLIENT.get_default_handler()
    LOGGER = logging.getLogger("PAGINATION")
    LOGGER.setLevel(logging.INFO)
    LOGGER.addHandler(HANDLER)
    error_client = error_reporting.Client()
    
    

    def __error_path(publisher, pub_obj_encoded, tries, url, error):
        """Function to handle possible errors on pagination

        Args:
            pub_obj_encoded ([dict]): [pubsub dict with infos of the page and tries]
            tries ([int]): [number of tries that this page was tried]
            url ([str]): [url to be parsed for pagination]
        """
        if tries < 5:
            publisher.publish(_THIS_FUNCTION_TOPIC, pub_obj_encoded)
        else:
            raise ConnectionError(
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

    # Object for failure
    pub_obj_encoded = json.dumps(
        {'url': url_decode, 'tries': tries}).encode("utf-8")

    response = requests.get(url_decode, headers=HEADERS)
    try:
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        # If the request has error page 500 follow error path
        if soup.select('title')[0].text == 'Error 500':
            raise HTTPError("500 Server Error: PAGE OFFLINE")
        else:
            # Next url soup object
            next_url = soup.select(_PAGINATION_CSS_SELECTOR)
            if next_url:
                # Loging if there is no next url and publish
                next_url = next_url[0].select('a')[0]['href']
                if not (next_url.startswith("http://") or next_url.startswith("https://")):
                    next_url = _BASE_URL + next_url
                pub_next_obj = json.dumps({"url": next_url})
                publisher.publish(_THIS_FUNCTION_TOPIC,
                                  pub_next_obj.encode('utf-8'))
            else:
                logging.info("Last url %s", url_decode)
            # Products <a/> attributes to be parsed
            products_soups = soup.select(_CHILD_CSS_SELECTOR)
            if not products_soups:
                raise Exception('Invalid value of products')
            products_url = [attribute['href']
                            for attribute in products_soups]

            # Publishing urls to the products topic
            for url in products_url:
                if not (url.startswith("http://") or url.startswith("https://")):
                    url = _BASE_URL + next_url
                product_obj = json.dumps({"url": url})
                publisher.publish(_DOWNLOAD_HTML_TOPIC,
                                  product_obj.encode('utf-8'))
    except HTTPError as error:
        if error.response.status_code == 403:
            __error_path(publisher, pub_obj_encoded,
                         tries, url_decode, error=403)
        else:
            logging.error(error)
            error_client.report_exception()
    except ConnectionError as error:
        logging.error("PAGE MAX TRIES: %s", error.message)
        error_client.report_exception()
    except Exception as error:
        logging.error(error)
        error_client.report_exception()
