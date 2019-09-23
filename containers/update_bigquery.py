import os
from google.cloud import bigquery
import requests 
from bs4 import BeautifulSoup
import time
client = bigquery.Client()
dataset = 'newdata'
table = 'rentaldata'

table_ref = client.dataset(dataset).table(table)
table = client.get_table(table_ref)
table_size = table.num_rows

field_url = [bigquery.schema.SchemaField('url', 'STRING', 'NULLABLE', None, ())]

list_urls = client.list_rows(table,selected_fields=field_url)

temp_table = '''#standardSQL
                CREATE TABLE  {dataset}.'temp_'{table}
                OPTIONS(
                expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
                ) AS
                SELECT corpus, COUNT(*) c
                FROM `bigquery-public-data.samples.shakespeare`
                GROUP BY corpus'''

def updated_list(delete_list):
   query =  ''' UPDATE {dataset}.{table}
                SET rental_deleted = 1
                WHERE url in (SELECT * FROM {dataset}.'temp_'{table})

            '''
deleted_list = []
for url in list_urls:
    response = requests.get(url)
    if response.status_code != 200:
        deleted_list.append(url)    
    # If the request has error page 500 follow error path
    soup = BeautifulSoup(response.content, 'lxml')
    if soup.select('title')[0].text == 'Error 500':
        deleted_list.append(url)
    if len(deleted_list) > 50:
        
    
