from google.cloud import storage
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup
import requests
import base64


def save_to_storage(data, context):
    publisher = pubsub_v1.PublisherClient()

    if 'data' in data:
        name = base64.b64decode(data['data']).decode('utf-8')
    url = name
    response = requests.get(url)
    # If the request fail send again to pubsub
    products_topic = 'projects/educare-226818/topics/child_scrape'
    if response.status_code != 200:
        publisher.publish(products_topic, url.encode('utf-8'))
    else:
	    # Get the ifle name
	    data_name = url.split('/')[-1]
	    storage_client = storage.Client(project='educare')
	    soup = BeautifulSoup(response.text)
	    if soup.select('title')[0].text == 'Error 500':
	    	publisher.publish(products_topic, url.encode('utf-8'))
	    else:
		    # Save to the blob
		    bucket_name = 'imoveis_data'
		    bucket = storage_client.get_bucket(bucket_name)
		    blob = bucket.blob(data_name)
		    blob.upload_from_string(response.content)
