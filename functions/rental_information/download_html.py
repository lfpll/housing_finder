from google.cloud import storage,pubsub_v1, error_reporting
from bs4 import BeautifulSoup
import requests
import base64

headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0'}


def download_html(data, context):
    """Receives an url on the data.data value
       Publish value to pub\sub if fails
       Download the page to google storage if it works

    Arguments:
            data {[type]} -- [description]
            context {[type]} -- [description]
    """
    try:
        # Getting the information from the page
        url = base64.b64decode(data['data']).decode('utf-8')
        response = requests.get(url, headers=headers)

        # If the request fail send again to pubsub
        products_topic = 'projects/educare-226818/topics/child_scrape'
        json_topic = 'projects/educare-226818/topics/html_path'
        publisher = pubsub_v1.PublisherClient()

        # If the status is not 200 the requestor was blocked
        if response.status_code != 200:
            publisher.publish(products_topic, url.encode('utf-8'))
        else:
            storage_client = storage.Client(project='educare')
            soup = BeautifulSoup(response.text,'lxml')
            # Checking for error in the page (Case of no http errors implemented)
            if soup.select('title')[0].text == 'Error 500':
                publisher.publish(products_topic, url.encode('utf-8'))
            else:
                # Saving the html by the url name
                data_name = url.split('/')[-1]

                # Opening the bucket connection
                bucket_name = 'imoveis-data'
                bucket = storage_client.get_bucket(bucket_name)
                blob = bucket.blob(data_name)
                blob.upload_from_string(response.content)
                # Publish path to be parsed
                publisher.publish(json_topic, data_name.encode('utf-8'))
    except Exception as error:
        error_client = error_reporting.Client()
        error_client.report_exception()
