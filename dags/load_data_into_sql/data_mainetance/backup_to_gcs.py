import pandas
from datetime import datetime
import logging 
import os
import pytz
import psycopg2

if "LOG_LEVEL" in os.environ and os.environ["LOG_LEVEL"] == 'INFO':
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


# Loadings the sql data that was modified into google cloud storage
if __name__ == "__main__":
    USER = os.environ["USER"]
    PWD = os.environ["SQL_PASSWORD"]
    IP = os.environ["IP"]
    ONLINE_TABLE = os.environ["TABLE_NAME"]
    DB = os.environ["DATABASE"]

    conn = psycopg2.connect(host=IP, database=DB,
                            user=USER, password=PWD)
    gcs_file_name = datetime.now(pytz.timezone(
            "America/Sao_Paulo")).strftime('-%Y-%m-%d-%Hhs')  
    
    logger.info('Getting data from SQL')
    stage_new = pandas.read_sql('SELECT * FROM %s WHERE TO_CHAR(NOW() :: DATE, \'yyyy-mm-dd\')::date <= date_stored::date'%ONLINE_TABLE,conn)
    stage_updates = pandas.read_sql('SELECT * FROM %s WHERE TO_CHAR(NOW() :: DATE, \'yyyy-mm-dd\')::date <= date_last_update::date'%ONLINE_TABLE,conn)
    stage_deleted = pandas.read_sql('SELECT * FROM %s WHERE TO_CHAR(NOW() :: DATE, \'yyyy-mm-dd\')::date <= date_deleted::date'%ONLINE_TABLE,conn)

    if not stage_new.empty:
        stage_new.to_parquet("gcs://backup-json/imvw_new_records%s"%gcs_file_name,allow_truncated_timestamps=True)
    
    if not stage_updates.empty:
        stage_updates.to_parquet("gcs://backup-json/imvw_updates_records%s"%gcs_file_name,allow_truncated_timestamps=True)

    if not stage_deleted.empty:        
        stage_deleted.to_parquet("gcs://backup-json/imvw_deleted_records_%s"%gcs_file_name,allow_truncated_timestamps=True)
    