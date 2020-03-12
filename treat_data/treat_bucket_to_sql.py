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

def treat_imovelweb_data(imovelweb_df:pd.DataFrame):
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

    # Remove spaces without afecting accent words
    def strip_with_utf8(string):
        if string is nan:
            return nan
        return regexp_non_words.sub('',string)

    #  Joining columns that have the same meaning
    tmp_df["banheiros"] = tmp_df[["banheiros","banheiro"]].bfill(axis=1).iloc[:, 0]
    tmp_df["vagas"] = tmp_df[["vagas","vaga"]].bfill(axis=1).iloc[:, 0]
    tmp_df["suites"] = tmp_df[["suites","suite"]].bfill(axis=1).iloc[:, 0]
    tmp_df["quartos"] = tmp_df[["quartos","quarto"]].bfill(axis=1).iloc[:, 0]
    tmp_df.drop(columns=["banheiro","vaga","suite","quarto"],inplace=True)

    # Correcting the creation date
    tmp_df['pub_data'] = tmp_df['pub_data'].apply(replace_pub_data).str.replace("[^0-9]","")

    # Removing "m2"
    tmp_df['area_util'] = tmp_df['area_util'].str.replace('m2','').astype("Float32")
    tmp_df['area_total'] = tmp_df['area_total'].str.replace('m2','').astype("Float32")

    # Removing spaces or bizarre values at the start or beggining of the string columns
    string_columns= tmp_df.select_dtypes('object').drop(columns=['additions','imgs'])
    tmp_df[string_columns.columns.values] = string_columns.applymap(strip_with_utf8)
    tmp_df[['bairro','cidade']] = tmp_df['bairro'].str.split(",",expand=True)
    return tmp_df


if __name__ == "__main__":
    # Instantiates a client of google storage
    storage_client = storage.Client()
    # Variables used for the connection to SQL
    USER = os.environ["USER"]
    PWD = os.environ["PASSWORD"]
    IP = os.environ["IP"]
    TABLE_NAME = os.environ["TABLE_NAME"]
    
    # Check if variables were declared
    if not USER < 0 or not PWD < 0 or not IP or not TABLE_NAME:
        raise ValueError("Invalid value for SQL connection enviroment variables.")
    # Reading the files from a json subfolder on the bucket in a list format
    json_list = get_json_into_list(bucket_name="imoveis-data-bigtable",subdir="stage",gcs_client=storage_client)
    
    # Doing some treatment for a better quality data
    treated_df = treat_imovelweb_data(imovelweb_df=pd.DataFrame(json_list))
    
    # Updating the table
    db_conn.execute(query)
    # Dumping treated data into stage table of SQL
    db_string = "postgres://{user}:{password}@{ip}/postgres".format(user=USER,fopassword=PWD,ip=IP)
    db_conn = create_engine(db_string)
    treated_df.to_sql(TABLE_NAME,db_conn,if_exists='replace')