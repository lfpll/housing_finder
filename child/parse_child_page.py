from google.cloud import storage
from google.cloud import pubsub_v1
from bs4 import BeautifulSoup
import requests
import base64
import hashlib


headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0'}

def save_to_storage(data, context):
	"""Receives an url on the data.data value
	   Publish value to pub\sub if fails
	   Download the page to google storage if it works

	Arguments:
			data {[type]} -- [description]
			context {[type]} -- [description]
	"""

	# Getting the information from the page
	name = base64.b64decode(data['data']).decode('utf-8')
	url = name
	response = requests.get(url,headers=headers)

	# If the request fail send again to pubsub
	products_topic = 'projects/educare-226818/topics/child_scrape'
	publisher = pubsub_v1.PublisherClient()

	# If the status is not 200 the requestor was blocked
	if response.status_code != 200:
		publisher.publish(products_topic, url.encode('utf-8'))
	else:
		# Get the filename name
		storage_client = storage.Client(project='educare')
		soup = BeautifulSoup(response.text)
		if soup.select('title')[0].text == 'Error 500':
			publisher.publish(products_topic, url.encode('utf-8'))
		else:
			# Saving the htl
			m = hashlib.md5()
			bucket_name = 'imoveis_data'
			bucket = storage_client.get_bucket(bucket_name)
			m.update(url)
			hash_id = str(int(m.hexdigest(), 16))
			blob = bucket.blob('/imoveis_web/{hash_id}'.format(hash_id=hash_id))
			blob.upload_from_string(response.content)
