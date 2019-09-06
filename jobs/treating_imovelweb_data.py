from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("treat_data").getOrCreate()

# First cast string values to integers or floats
df.select('vagas').distinct().show()


df = spark.read.json("/home/luizlobo/Documentos/code/parse_imoveis/tests/samples/gcloud/")
# First tread the data in strings removing starting spaces unused
str_columns = [column_name for column_name,data_type in df.dtypes if data_type == 'string']
for column in str_columns:
    df = df.withColumn(column,regexp_replace(regexp_replace(column,"\s+"," "),"^[^\\p{L}\\p{N}]|[^\\p{L}\\p{N}]$","")) 

# Removing the square meter from the  area
df = df.withColumn("area_util",regexp_replace("area_util","m2",""))
df = df.withColumn("area_total",regexp_replace("area_total","m2",""))

# Transforming the latitue and longitue into one tuple
df = df.withColumn("geopoint",struct("latitude","longitude"))
df = df.drop('latitude','longitude')

# Treating the publication date and transforming into an integer 
def replace_pub_data(prefix):
    return prefix.lower().replace('hoje','0').replace('ontem','1')

replace_udf = udf(replace_pub_data)
# The replacing, the order of regexp_replace and replace_udf is important
df.withColumn("pub_data",regexp_replace(replace_udf("pub_data"),"[^0-9]","").select("pub_data").distinct().show()

spark.stop()    