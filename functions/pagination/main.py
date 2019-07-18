from bs4 import BeautifulSoup
from google.cloud import pubsub_v1,error_reporting
import requests
import base64
import os

out_topic = os.environ['OUT_TOPIC']


def parse_and_paginate(data, context):
    name = base64.b64decode(data['data']).decode('utf-8')
    publisher = pubsub_v1.PublisherClient()

    url = name
    response = requests.get(url)

    pagination_topic = 'projects/educare-226818/topics/scrape'
    # If the request fail send again to pubsub
    if response.status_code != 200:
        publisher.publish(pagination_topic, url.encode('utf-8'))

    # Next page

    father_url = "https://www.imovelweb.com.br"
    soup = BeautifulSoup(response.content,'lxml')
    next_url = soup.select('li.pag-go-next')
    if len(next_url) > 0:
        next_url = next_url[0].select('a')[0]['href']
        publisher.publish(pagination_topic, (father_url + next_url).encode('utf-8'))

    # Pages of producst
    if soup.select('title')[0].text == 'Error 500':
        publisher.publish(out_topic, url.encode('utf-8'))
    else:
        products_soups = soup.select('a.go-to-posting')
        if len(products_soups) <= 0:
            raise Exception('Invalid value of products')
        products_url = [father_url + attribute['href']
                        for attribute in products_soups]

        # Publishing urls to the products topic
        for product in products_url:
            publisher.publish(out_topic, product.encode('utf-8'))