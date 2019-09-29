import sys
import subprocess
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,coalesce,col,regexp_replace,split,trim,struct
from pyspark.sql.types import *

def get_number_columns(DF):
    """A function that receives a spark dataframe and 
        returns the columns that are strings and can be numbers
    Arguments:
        DF {[spark.dataframe]} = dataframe to get columss
    Returns:
        [type]: [description]
    """
    dict_schema = {key:val for key,val in DF.dtypes}
    json_schema = [col_name for col_name,dtype in dict_schema.items() if dtype == 'string']
    # Return a list of booleans for each row to  check if can be casted to float (Integer types are the same price as float on bigquery)
    numbers_list =  [DF.select(col,col(col).cast("float").isNotNull().
                        alias(col+"_1")).dropna().select(col+"_1") for col in json_schema]
    # Getting the unique values of the bolleans
    unique_booleans = [unique.distinct().collect() for unique in numbers_list]
    # Getting the ones that can only have one value -> (false or true) not [false,true] and this value is true
    single_bools_lists = filter(lambda list_bool: (len(list_bool) <= 1 and list_bool[0]),unique_booleans)
    # Transforming the list of rows into a columdf
    # ns string names and removing the "_1" from the name
    row_as_dictionary = map(lambda val: val[0].asDict(),single_bools_lists)
    only_trues_list = filter(lambda val:val.values()[0],row_as_dictionary)
    column_names = [dictRow.keys()[0].replace('_1','') for dictRow in only_trues_list]
    return column_names

spark = SparkSession.builder.appName("treat_data").getOrCreate()

input_path = sys.argv[1]
out_path = sys.argv[2]
output_dataset = sys.argv[3]
output_table = sys.argv[4]

df = spark.read.json(input_path)

regexp_non_words = re.compile(r'^\W+|\W+$',flags=re.UNICODE)

def remove_non_utf8(string):
    return regexp_non_words.sub('',string)

# replace_utf8 = udf(remove_non_utf8)

# First treat the data in strings removing starting spaces unused
str_columns = [column_name for column_name,data_type in df.dtypes if data_type == 'string']
for column in str_columns:
    df = df.withColumn(column,regexp_replace(remove_non_utf8(column),r"\s+"," "))

# Removing the square meter from the  area
df = df.withColumn("area_util",regexp_replace("area_util","m2",""))
df = df.withColumn("area_total",regexp_replace("area_total","m2",""))

# Transforming the latitue and longitue into one tuple
df = df.withColumn("geopoint",struct("latitude","longitude"))


# Treating the publication date and transformi
# ng into an integer 
def replace_pub_data(prefix):
    return prefix.lower().replace('hoje','0').replace('ontem','1')

replace_udf = udf(replace_pub_data)
df = df.withColumn("pub_data",regexp_replace(replace_udf("pub_data"),"[^0-9]",""))

df = df.withColumn("banheiros",coalesce("banheiros","banheiro"))
df = df.withColumn("vagas",coalesce("vaga","vagas"))
df = df.withColumn("suites",coalesce("suites","suite"))
df = df.withColumn("quartos",coalesce("quarto","quartos"))

split_col = split(df['bairro'], ',')
df = df.drop('latitude','longitude','quarto','vaga','suite','banheiro')
df = df.withColumn("cidade",trim(split_col.getItem(1)))
df = df.withColumn("bairro",trim(split_col.getItem(0)))
df.coalesce(1).write.option("codec", "org.apache.hadoop.io.compress.GzipCodec").parquet(out_path)

subprocess.check_call(
    'bq load --source_format PARQUET '
    '--replace '
    '--autodetect '
    '{dataset}.{table} {files}'.format(
        dataset=output_dataset, table=output_table, files=out_path+'/part-*'
    ).split())
