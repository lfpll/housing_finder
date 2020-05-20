import os
import re
import logging
from datetime import datetime
import subprocess
import json
import argparse
import pytz
import pandas as pd
from google.cloud import storage
from numpy import nan
from sqlalchemy import create_engine


def get_json_into_list(bucket_name: str, subdir: str, gcs_client):
    """Go into a subdirectory in bucket and returns a pack of json in a list of dicts

    Arguments:
        bucket_name [str] -- [name of the bucket]
        subdir [str] -- [name of the subdirectories]

    Returns:
        [list(dict)] -- [list of dicts with the data of the jsons]
    """
    bucket = gcs_client.get_bucket(bucket_name)
    blobs_list = bucket.list_blobs(prefix=subdir)
    json_list = []
    for blob in blobs_list:
        json_list.append(json.loads(blob.download_as_string()))
    return json_list


def treat_imovelweb_data(imovelweb_df):
    """Specific function created to treat from the imovelweb site data

    Arguments:
        imovelweb_df {pd.DataFrame} -- [data frame got from joining the json]

    Returns:
        [pd.DataFrame] -- [returns the dataframe treated]
    """
    tmp_df = imovelweb_df.copy()
    regexp_non_words = re.compile(r'^\W+|\W+$', flags=re.UNICODE)
    # Remove spaces without affecting accent words

    def strip_with_utf8(string):
        if string is nan:
            return nan
        return regexp_non_words.sub('', string.replace('\n', ' '))

    #  Joining columns that have the same meaning
    tmp_df["banheiros"] = tmp_df[["banheiros", "banheiro"]].bfill(
        axis=1).iloc[:, 0]
    tmp_df["vagas"] = tmp_df[["vagas", "vaga"]].bfill(axis=1).iloc[:, 0]
    tmp_df["suites"] = tmp_df[["suites", "suite"]].bfill(axis=1).iloc[:, 0]
    tmp_df["quartos"] = tmp_df[["quartos", "quarto"]].bfill(axis=1).iloc[:, 0]

    # Removing "m2"
    tmp_df['area_util'] = tmp_df['area_util'].str.replace(
        'm2', '').astype("Float32")
    tmp_df['area_total'] = tmp_df['area_total'].str.replace(
        'm2', '').astype("Float32")

    # Removing spaces or bizarre values at the start or beggining of the string columns
    string_columns = tmp_df.select_dtypes(
        'object').drop(columns=['additions', 'imgs'])
    tmp_df[string_columns.columns.values] = string_columns.applymap(
        strip_with_utf8)
    tmp_df[['bairro', 'cidade']] = tmp_df['bairro'].str.split(",", expand=True)
    tmp_df["page_url"] = tmp_df["url"]

    # Cleaning the dataframe
    tmp_df.drop(columns=["banheiro", "vaga", "suite", "quarto",
                         "pub_data", "pub_anun", "pub_code", "url"], inplace=True)

    return tmp_df

logger = logging.getLogger('update_sql_table')

if __name__ == "__main__":
    # Instantiates a client of google storage

    STORAGE_CLIENT = storage.Client()
    # Variables used for the connection to SQL
    USER = os.environ["USER"]
    PWD = os.environ["SQL_PASSWORD"]
    IP = os.environ["IP"]
    STAGE_TABLE = os.environ["STAGE_TABLE_NAME"]
    DB = os.environ["DATABASE"]

    BUCKET_NAME="imoveis-data-bigtable"
    SUBDIR="stage"
    logger.debug("USER:{0}\nTable:{1}\nDatabase:{2}".format(
        USER, STAGE_TABLE, DB))

    if not USER or not PWD or not IP or not STAGE_TABLE:
        raise ValueError(
            "Invalid value for SQL connection enviroment variables.")
    
    logger.info("Starting ingesting files from gcs")
    # Reading the files from a json subfolder on the bucket in a list format
    json_list = get_json_into_list(
        bucket_name=BUCKET_NAME, subdir=SUBDIR, gcs_client=STORAGE_CLIENT)

    if len(json_list) != 0:
        logger.info("Number of records: " + str(len(json_list)))

        # Doing some treatment for a better quality data
        treated_df = treat_imovelweb_data(imovelweb_df=pd.DataFrame(json_list))

        # Dumping treated data into stage table of sql
        db_string = "postgres://{user}:{password}@{ip}/{database}".format(user=USER, password=PWD, ip=IP, database=DB)
        logger.debug("Database connection string %s" % db_string)
        db_conn = create_engine(db_string)
        treated_df.to_sql(STAGE_TABLE, db_conn, if_exists='append',index=False)
    else:
        logger.info("No Records found on Google Cloud Storage at gs://{0}/{1}".format(BUCKET_NAME,SUBDIR))
