dataset="newdata"
table="rentaldata"
out_table="delete"
out_bucket="imoveis-data/stage"

python list_deleted_rentals.py \ 
--table=$table --dataset=$dataset --out_bucket=$out_bucket

# Loading the data into bigquery
bq load --source_format PARQUET --noreplace \
--schema $schema_path  --autodetect $dataset.$out_table $out_bucket'/delete/*.json'

query="UPDATE $dataset.$table SET rental_deleted=1, delete_date=CURRENT_DATE(\"America/Bahia\") WHERE $table.url in (SELECT url from \"$dataset.$out_table\")"
bq query $query