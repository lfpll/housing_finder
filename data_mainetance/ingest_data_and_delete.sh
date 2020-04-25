export USER="postgres"
export IP="0.0.0.0"
export TABLE_NAME="imoveis_online"
export STAGE_TABLE_NAME="imoveis_stage"
export OFFLINE_TABLE_NAME="imoveis_offline"
export SERVER="postgres"
export DATABASE="postgres"
export LOG_LEVEL="INFO"

python3 ./treat_bucket_to_sql.py
python3 ./remove_offline_urls_from_sql.py
python3 ./backup_to_gcs
sudo shutdown
