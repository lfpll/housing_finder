import base64
import json
import os
from google.cloud import storage, pubsub_v1, error_reporting
from bs4 import BeautifulSoup
import requests


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0'}

# Bucket where the data will be stored
_IN_BUCKET = os.environ["DELIVER_BUCKET"]
# 'projects/educare-226818/topics/child_scrape'
_THIS_FUNCTION_TOPIC = os.environ["THIS_TOPIC"]
# 'projects/educare-226818/topics/html_path'
_PARSE_FUNCTION_TOPIC = os.environ["PARSE_TOPIC"]


def download_page(message, context):
    """Receives a message object from pubsub, on the key 'data' it retrieves an url
        Download this url into the _IN_BUCKET or republish if it fails

    Arguments:
            data {[base64 encoded string]} -- object json encoded with data:{file_path}
            context {[object]} -- [description]
    """
    try:
        # Getting the url to be paginated

        data = base64.b64decode(message['data']).decode('utf-8')
        json_decoded = json.loads(data)
        url = json_decoded['url']
        response = requests.get(url, headers=HEADERS)
        publisher = pubsub_v1.PublisherClient()

        # If the status is not 200 the requestor was blocked send back
        if response.status_code != 200:
            publisher.publish(_THIS_FUNCTION_TOPIC, url.encode('utf-8'))
        else:
            storage_client = storage.Client()
            soup = BeautifulSoup(response.text, 'lxml')

            # Special case where this website bad implemented http errors
            if soup.select('title')[0].text == 'Error 500':
                publisher.publish(_THIS_FUNCTION_TOPIC, url.encode('utf-8'))
            else:
                # Saving the html by the url name
                file_name = url.split('/')[-1]
                pub_obj_encoded = json.dumps(
                    {'file_path': file_name, 'url': url}).encode("utf-8")

                # Opening the bucket connection
                bucket = storage_client.get_bucket(_IN_BUCKET)
                blob = bucket.blob(file_name)
                blob.upload_from_string(response.content)

                # Publish path to be parsed and transformed to json
                publisher.publish(_PARSE_FUNCTION_TOPIC, pub_obj_encoded)

    except Exception:
        error_client = error_reporting.Client()
        error_client.report_exception()
