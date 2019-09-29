import sqlite3
import pandas as pd
from sqlite3 import Error


class Mock_Sql_conn:
    def __init__(self, db_file):
        self.conn = self.__create_connection(db_file)

    def __create_connection(self, db_file):
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            return conn
        except Error as e:
            print(e)

    def load_mock_data_parquet(self, data_path):
        df = pd.read_parquet(data_path)
        df.to_sql(name='mock', con=self.conn)

    def close_conn(self):
        self.conn.close()

