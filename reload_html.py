# Reloading the blobs into the pubsub

from google.cloud import pubsub_v1,storage

list_blobs = []
string_split = '/b/imoveis-data/o/'


topic = 'projects/educare-226818/topics/html_path'
bucket_name = 'imoveis-data'
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

# Getting the path of the blobs into a list
for blob in bucket.list_blobs():
    path = ''.join(blob.media_link.split('/')[-1].split('?')[:-1]).encode('utf-8')
    list_blobs.append((path))

publisher = pubsub_v1.PublisherClient()
for blob_path in list_blobs:
    publisher.publish(topic=topic,data=blob_path)