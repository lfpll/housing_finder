import sys
import os
from pyspark.sql import SparkSession,functions as F
from pyspark.sql.types import BooleanType



input_path = sys.argv[1]
output_path = sys.argv[2]

# THIS IS A BAD JOB, I'm using because it's for an small project and runned once
def set_new_schema(name,type_name):
    """A function that receives the name of the column and the type as spark defines
       And converts to the naming convetiong of bigquery json schema
    
    Arguments:
        name {str} -- [name of the column]
        type_name {str} -- [type of the column]
    
    Returns:
        bq_schema[type] -- [description]
    """
    # Note that description is mising
    bq_schema = {"name":name,"type":"","mode":"NULLABLE"}
    type_name = type_name.replace("double","float")
    # Checking if is a nested list 
    if type_name.startswith("array"):
        type_name = type_name.lower().replace("array<","").replace(">","")
        bq_schema["mode"] = "REPEATED"
    bq_schema["type"] = type_name
    return bq_schema

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    jsonDF = spark.read.json((spark.read.text(input_path).repartition(1000).rdd))
    dict_schema = {key:val for key,val in jsonDF.dtypes}
    json_schema = [col_name for col_name,dtype in dict_schema.items() if dtype == 'string']


    # Return a list of booleans for each row to check if can be casted to float (Integer types are the same price as float on bigquery)
    numbers_list =  [jsonDF.select(col,F.col(col).cast("float").isNotNull().alias(col+"_1")).dropna().select(col+"_1") for col in json_schema]

    # Getting the unique values of the bolleans
    unique_booleans = [unique.distinct().collect() for unique in numbers_list]

    # Getting the ones that can only have one value -> (false or true) not [false,true]
    single_bools_lists = filter(lambda list_bool: (len(list_bool) <= 1 and list_bool[0]),unique_booleans)

    # Transforming the list of rows into a columns string names and removing the "_1" from the name
    row_as_dictionary = map(lambda val: val[0].asDict(),single_bools_lists)

    only_trues_list = filter(lambda val:val.values()[0],row_as_dictionary)
    column_names = [dictRow.keys()[0].replace('_1','') for dictRow in only_trues_list]

    # Replacing in the schema
    for column in column_names:
        dict_schema[column] = 'float'

    bqFinalSchema = [set_new_schema(key,val) for key,val in dict_schema.items()]
    columns = ["name","type","mode"]
    df = spark.createDataFrame(bqFinalSchema,columns)
    df.coalesce(1).write.mode("overwrite").json(output_path)
    spark.stop()