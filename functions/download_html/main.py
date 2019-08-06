from google.cloud import storage,pubsub_v1, error_reporting
from bs4 import BeautifulSoup
import requests
import base64
import json

headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0'}

# Bucket where the data will be stored
_IN_BUCKET = os.environ["DELIVER_BUCKET"]
_THIS_FUNCTION_TOPIC = os.environ["THIS_TOPIC"] # 'projects/educare-226818/topics/child_scrape'
_PARSE_FUNCTION_TOPIC = os.environ["PARSE_TOPIC"] # 'projects/educare-226818/topics/html_path'
_PROJECT_NAME = os.environ['PROJECT_NAME'] # educare

# TODO add max retries to publish
def download_page(data, context):
    """Receives an url on the data.data value
       Publish value to pub\sub if fails
       Download the page to google storage if it works

    Arguments:
            data {[base64 encoded string]} -- object json encoded with data:{file_path}
            context {[object]} -- [description]
    """
    try:
        # Getting the url to be paginated

        url = base64.b64decode(data['data']).decode('utf-8')
        response = requests.get(url, headers=headers)    
        publisher = pubsub_v1.PublisherClient(_PROJECT_NAME)

        
        # If the status is not 200 the requestor was blocked send back
        if response.status_code != 200:
            publisher.publish(_THIS_FUNCTION_TOPIC, url.encode('utf-8'))
        else:
            storage_client = storage.Client(_PROJECT_NAME)
            soup = BeautifulSoup(response.text,'lxml')
            
            # Special case where this website bad implemented http errors
            if soup.select('title')[0].text == 'Error 500':
                publisher.publish(sub_topic, url.encode('utf-8'))
            else:
                # Saving the html by the url name
                file_name = url.split('/')[-1]
                pub_obj_encoded = json.dumps({'file_path':file_name,'url':url}).
                                    encode("utf-8")
                
                # Opening the bucket connection
                bucket = storage_client.get_bucket(_IN_BUCKET)
                blob = bucket.blob(file_name)
                blob.upload_from_string(response.content)
                
                # Publish path to be parsed and transformed to json
                publisher.publish(_PARSE_FUNCTION_TOPIC, pub_obj_encoded)

    except Exception as error:
        error_client = error_reporting.Client()
        error_client.report_exception()
