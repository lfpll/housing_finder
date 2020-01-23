dataset="newdata"
table="rentaldata"
out_table="delete"
out_bucket="imoveis-data/stage"

python list_deleted_rentals.py --table=$table --dataset=$dataset --out_bucket=$out_bucket

# Treating the data with spark
gcloud dataproc clusters create dumdataproc --region us-central1 --num-workers 2 --worker-machine-type custom-2-5120

# Treat the data and join the correct columns
gcloud dataproc jobs submit pyspark --cluster dumdataproc treating_imovelweb_data.py -- $input_path $output_path $dataset $table
gcloud dataproc clusters delete dumdataproc --region us-central1

# Loading the data into bigquery
bq load --source_format PARQUET --noreplace --schena $schema_path  --autodetect $dataset.$out_table $out_bucket'/delete/*.json'

query="UPDATE $dataset.$table SET rental_delete=1 WHERE $table.url in (SELECT url from $dataset.$out_table)"
