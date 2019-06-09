from bs4 import BeautifulSoup
from google.cloud import pubsub_v1
import requests
import base64


def parse_head_page(data, context):
    """ Parge the main page
        Passing to the current pub\subs for each kind of page to be parsed


    Arguments:
        data {[type]} -- [description]
        context {[type]} -- [description]
    """
    name = base64.b64decode(data['data']).decode('utf-8')
    publisher = pubsub_v1.PublisherClient()

    url = name
    response = requests.get(url)

    pagination_topic = 'projects/educare-226818/topics/scrape'
    # If the request fail send again to pubsub
    if response.status_code != 200:
        publisher.publish(pagination_topic, url.encode('utf-8'))
    else:
        # Next page

        father_url = "https://www.imovelweb.com.br"
        soup = BeautifulSoup(response.content)
        next_url = soup.select('li.pag-go-next')
        # Check to see if it's incorrect page
        if soup.select('title')[0].text == 'Error 500':
            publisher.publish(pagination_topic, url.encode('utf-8'))
        else:
            if len(next_url) > 0:
                next_url = next_url[0].select('a')[0]['href']
                publisher.publish(pagination_topic, (father_url+next_url).encode('utf-8'))
            products_topic = 'projects/educare-226818/topics/child_scrape'
            # Pages of producst
            products_soups = soup.select('a.go-to-posting')
            products_url = [father_url+attribute['href']
                            for attribute in products_soups]

            # Publishing urls to the products topic
            for product in products_url:
                publisher.publish(products_topic, product.encode('utf-8'))
