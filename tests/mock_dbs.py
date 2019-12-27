import sqlite3
import pandas as pd
from sqlite3 import Error
import pytest
import os


class Mock_Sql_conn:
    def __init__(self, db_file):
        if os.path.exists(db_file):
            os.remove(db_file)
        self.conn = self.__create_connection(db_file)

    def __create_connection(self, db_file):
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            print(e)

    def load_mock_data_parquet(self, data_path):
        df = pd.read_parquet(data_path)
        df.to_sql(name='mock', con=self.conn)

    def query_database(self, query):
        return self.conn.execute(query)

    def close_conn(self):
        self.conn.close()


class Mock_Client_BigQuery:

    def __init__(self, mock_sql_conn, credentials=''):
        self.data = mock_sql_conn
        self.table = 'mock'

    def list_rows(self, table,selected_fields):
        columns = [schema_field.name for schema_field in selected_fields]
        cols_sql_query = ','.join(columns)
        query = 'select {0} from {1}'.format(cols_sql_query, table)
        return self.data.query_database(query).fetchall()

    def get_table(self,table):
        return self.table

class Mock_blob:
    def __init__(self, blob_path):
        self.blob_path = blob_path

    def upload_from_string(self,text):
        with open(self.blob_path) as mock_file:
            mock_file.write(text)

class Mock_bucket:
    def __init__(self, bucket):
        self.bucket_name = bucket_name
        self.bucket_path = os.getcwd() + "\\tmp\\" + bucket_name
        __mock_folder()

    # Creating a temp folder to mock the bucket
    def __mock_folder(self):
        if not os.path.exists(self.bucket_path):
            os.makedirs(self.bucket_path)
        
    def blob(self,path):
        self.blob = Mock_blob(self.bucket_path+'\\'+path)

class Mock_storage_client:
    def __init__(self):
        pass
                
    def get_bucket(self,bucket):
        return Mock_bucket(bucket)
