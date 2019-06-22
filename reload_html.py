from google.cloud import pubsub_v1,storage

list_blobs = []
string_split = '/b/imoveis-data/o/'


topic = 'projects/educare-226818/topics/child_scrape'
bucket_name = 'imoveis_data'
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)

for blob in bucket.list_blobs():
    path = blob.path.split(bucket_name)[-1]
    gs_path = 'gs://{bucket_name}/{path}'.format(bucket_name=bucket_name,path=path)
    list_blobs.append(gs_path.encode('utf-8'))

publisher = pubsub_v1.PublisherClient()
for blob_path in list_blobs:
    import pdb;pdb.set_trace()
    publisher.publish(topic=topic,data=blob_path)