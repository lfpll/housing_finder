import pandas
from datetime import datetime
import logging 
import os
import pytz
import psycopg2

if "LOG_LEVEL" in os.environ and os.environ["LOG_LEVEL"] == 'INFO':
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    USER = os.environ["USER"]
    PWD = os.environ["SQL_PASSWORD"]
    IP = os.environ["IP"]
    ONLINE_TABLE = os.environ["TABLE_NAME"]
    OFFLINE_TABLE = os.environ["TABLE_NAME"]
    DB = os.environ["DATABASE"]
    conn = psycopg2.connect(host=IP, database=DB,
                            user=USER, password=PWD)
    gcs_file_name = datetime.now(pytz.timezone(
            "America/Sao_Paulo")).strftime('imoveisweb-%Y-%m-%d-%Hhs')  
    pandas.read_sql('SELECT * FROM %s'%ONLINE_TABLE,conn).to_parquet("gcs://backup-json/imoveis_online_%s"%gcs_file_name,allow_truncated_timestamps=True)
    logger.info('Data online upload to backup')
    pandas.read_sql('SELECT * FROM %s'%OFFLINE_TABLE,conn).to_parquet("gcs://backup-json/imoveis_offline_%s"%gcs_file_name,allow_truncated_timestamps=True)
    logger.info('Data offline upload to backup')