import base64
import json
import os
from google.cloud import storage, pubsub_v1, error_reporting, logging as cloud_logging
import logging
from bs4 import BeautifulSoup
import requests



# Instantiating log client
LOG_CLIENT = cloud_logging.Client()
handler = LOG_CLIENT.get_default_handler()
LOGGER = logging.getLogger('cloudLogger')
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(handler)


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0'}

# Bucket where the data will be stored
_OUT_BUCKET = os.environ["DELIVER_BUCKET"]
_JSON_BUCKET = os.environ["JSON_BUCKET"]
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
            raise Exception(
                "%s was already parsed 5 times, ended with %s page"%(url, error))
    try:
        # Getting the url to be paginated        
        data = base64.b64decode(message['data']).decode('utf-8')
        json_decoded = json.loads(data)
        url = json_decoded['url']
        
        # Out file name to gsbucket
        file_name = url.split('/')[-1]

        storage_client = storage.Client()
    
        
        bucket = storage_client.get_bucket(_OUT_BUCKET)
        blob = bucket.blob(file_name)
        # Checking if file exists, meaning it was an URL recreated
        if blob.exists():
            bucket = storage_client.get_bucket(_OUT_BUCKET)
            blob = bucket.blob('update/{0}'.format(file_name))
            blob.upload_from_string('')
        else:
            response = requests.get(url, headers=HEADERS)
            publisher = pubsub_v1.PublisherClient()

            # Adding number o tries
            tries = 0
            if 'tries' in json_decoded:
                tries = int(json_decoded['tries']) + 1

            # Object for failture
            pub_obj_encoded = json.dumps({'url': url, 'tries': tries}).encode("utf-8")

            # If the status is not 200 the requestor was blocked send back
            if response.status_code != 200:
                __error_path(publisher,pub_obj_encoded,tries,url,error=response.status_code)
            else:
                
                soup = BeautifulSoup(response.text, 'lxml')

                # Special case where this website bad implemented http errors
                if soup.select('title')[0].text == 'Error 500':
                    __error_path(publisher,pub_obj_encoded,tries,url,error=500)
                    publisher.publish(_THIS_FUNCTION_TOPIC, url.encode('utf-8'))
                else:
                    # Saving the html by the url name
                    
                    pub_obj_encoded = json.dumps(
                        {'file_path': file_name, 'url': url}).encode("utf-8")

                    # Opening the bucket connection
                    
                    blob.upload_from_string(response.content)

                    # Publish path to be parsed and transformed to json
                    publisher.publish(_PARSE_FUNCTION_TOPIC, pub_obj_encoded)
    except Exception:
        error_client = error_reporting.Client()
        error_client.report_exception()
