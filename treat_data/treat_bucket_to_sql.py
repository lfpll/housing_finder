# Imports the Google Cloud client library
from google.cloud import storage
import json
import pandas as pd
import re
from numpy import nan
import os
from sqlalchemy import create_engine
import logging


# Return batch of json in a bucket subfolder
def get_json_into_list(bucket_name:str ,subdir:str,gcs_client):
    """Go into a subdirectory in bucket and returns a pack of json in a list
    
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
    """Specific class created to treate from the imovelweb site data
    
    Arguments:
        imovelweb_df {pd.DataFrame} -- [data frame got from joining the json]
    
    Returns:
        [pd.DataFrame] -- [returns the dataframe treated]
    """    
    tmp_df = imovelweb_df.copy()
    regexp_non_words = re.compile(r'^\W+|\W+$',flags=re.UNICODE)
    # Treating the publication date and transforming into an integer 
    def replace_pub_data(prefix):
        return prefix.lower().replace('hoje','0').replace('ontem','1')

    # Remove spaces without affecting accent words
    def strip_with_utf8(string):
        if string is nan:
            return nan
        return regexp_non_words.sub('',string.replace('\n',' '))

    #  Joining columns that have the same meaning
    tmp_df["banheiros"] = tmp_df[["banheiros","banheiro"]].bfill(axis=1).iloc[:, 0]
    tmp_df["vagas"] = tmp_df[["vagas","vaga"]].bfill(axis=1).iloc[:, 0]
    tmp_df["suites"] = tmp_df[["suites","suite"]].bfill(axis=1).iloc[:, 0]
    tmp_df["quartos"] = tmp_df[["quartos","quarto"]].bfill(axis=1).iloc[:, 0]

    # Correcting the creation date
    tmp_df['pub_data'] = tmp_df['pub_data'].apply(replace_pub_data).str.replace("[^0-9]","")
    
    # Removing "m2"
    tmp_df['area_util'] = tmp_df['area_util'].str.replace('m2','').astype("Float32")
    tmp_df['area_total'] = tmp_df['area_total'].str.replace('m2','').astype("Float32")
    
    # Removing spaces or bizarre values at the start or beggining of the string columns
    string_columns= tmp_df.select_dtypes('object').drop(columns=['additions','imgs'])
    tmp_df[string_columns.columns.values] = string_columns.applymap(strip_with_utf8)
    tmp_df[['bairro','cidade']] = tmp_df['bairro'].str.split(",",expand=True)
    tmp_df["page_url"] = tmp_df["url"]
    
    # Cleaning the dataframe
    tmp_df.drop(columns=["banheiro","vaga","suite","quarto","pub_data","pub_anun","pub_code","url"], inplace=True)

    return tmp_df

def execute_query_from_file(query_file_path,conn):
    query = open(query_file_path).read().replace('\n',' ')
    conn.execute(query)


logger = logging.getLogger('update_sql_table')
if "LOG_LEVEL" in os.environ:
    logger.setLevel(os.environ["LOG_LEVEL"])

if __name__ == "__main__":
    # Instantiates a client of google storage
    storage_client = storage.Client()
    # Variables used for the connection to SQL
    USER = os.environ["USER"]
    PWD = os.environ["PASSWORD"]
    IP = os.environ["IP"]
    TABLE_NAME = os.environ["TABLE_NAME"]
    DB = os.environ["DATABASE"]
    
    logger.debug("USER:{0}\nTable:{1}\nDatabase:{2}".format(USER,TABLE_NAME,DB))

    # Check if variables were declared
    if not USER or not PWD or not IP or not TABLE_NAME:
        raise ValueError("Invalid value for SQL connection enviroment variables.")
    # Reading the files from a json subfolder on the bucket in a list format
    json_list = get_json_into_list(bucket_name="imoveis-data-bigtable",subdir="stage",gcs_client=storage_client)
    logger.info("Number of records: " +str(len(json_list)))
    # Doing some treatment for a better quality data
    treated_df = treat_imovelweb_data(imovelweb_df=pd.DataFrame(json_list))
    
    # Dumping treated data into stage table of SQL
    db_string = "postgres://{user}:{password}@{ip}/{database}".format(user=USER,password=PWD,ip=IP,database=DB)
    logger.debug("Database connection string %s"%db_string)
    db_conn = create_engine(db_string)
    treated_df.to_sql(TABLE_NAME,db_conn,if_exists='append')
    # Executing the queries of update and insert of the data
    execute_query_from_file('./update_denormalized.sql',db_conn)
    execute_query_from_file('./insert_denormalized.sql',db_conn)