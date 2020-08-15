from datetime import timedelta,datetime
import pytz
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# SQL table names
IMOVEIS_TABLE='imoveis_online'
STAGE_DATA_TABLE='imoveis_stage'
STAGE_NEW_DATA='stage_imoveis_novos'
STAGE_UPDATE_DATA='stage_imoveis_update'
TMP_URLS_TABLE='tmp_offline_urls'
POSTGRES_IP="0.0.0.0"

STAGE_GCS_FOLDER="gs://imoveis-data-bigtable/stage"

default_args = {
    'start_date': datetime(2020, 6, 27),
    'owner': 'airflow',
    'depends_on_past': True,
    'email': ['luizfpll@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


dag_ingest = DAG(
    'ingest_rental',
    catchup=False,
    default_args=default_args,
    description='DAG that treat_Data, load into SQL and store on GCS',
    schedule_interval='0 9 * * *'
)


default_args['dag'] = dag_ingest

date_today = datetime.now(pytz.timezone(
        "America/Sao_Paulo")).strftime('%Y-%m-%d-%Hhs')


USER = 'postgres'
DATABASE = 'postgres'

run_ingest_python= """
                    export SQL_PASSWORD={PWD}
                    export USER="{USER}"
                    export IP="{POSTGRES_IP}"
                    export STAGE_TABLE_NAME="{STAGE_TABLE}"
                    export DATABASE="{DB}"

                    source ~/venv/bin/activate
                    python3 ~/data_mainetance/ingest_new_data.py

                """.format(PWD=os.environ['SQL_PWD']
                              ,USER=USER
                              ,POSTGRES_IP=POSTGRES_IP
                              ,STAGE_TABLE=STAGE_DATA_TABLE
                              ,DB=DATABASE)

ingest_delete_urls="""
                    export SQL_PASSWORD={PWD}
                    export USER="{USER}"
                    export IP="{POSTGRES_IP}"
                    export TABLE_NAME="{ONLINE_TABLE}"
                    export DATABASE="{DB}"

                    source ~/venv/bin/activate
                    python3 ~/data_mainetance/get_offline_urls.py
                    """.format(PWD=os.environ['SQL_PWD']
                               ,USER=USER
                               ,POSTGRES_IP=POSTGRES_IP
                               ,ONLINE_TABLE=IMOVEIS_TABLE
                               ,DB=DATABASE)


ingest_new_data = SSHOperator(
    default_args=default_args,
    task_id="ingesting_new_data",
    ssh_conn_id="ssh_python",
    command=run_ingest_python
)   


get_offline_urls = SSHOperator(
    default_args=default_args,
    task_id="get_offline_urls",
    ssh_conn_id="ssh_python",
    command=ingest_delete_urls
)


# SQL queries
separate_new_data = PostgresOperator(
    default_args=default_args,
    task_id="stage_new_and_update_data",
    postgres_conn_id="postgres_db",
    sql="/sql/insert_stage_data.sql",
    database=DATABASE
)

update_online_table = PostgresOperator(
    default_args=default_args,
    task_id="upsert_table",
    postgres_conn_id="postgres_db",
    sql="/sql/insert_new_data.sql",
    database=DATABASE
)

clean_stage_tables = PostgresOperator(
    default_args=default_args,
    task_id="clean_stage_tables",
    postgres_conn_id="postgres_db",
    sql="/sql/delete_offline_data.sql",
    database=DATABASE       
)

# Uploading daily updates to GCS
upload_data_to_gcs="""
                    export SQL_PASSWORD={PWD}
                    export USER="{USER}"
                    export IP="{POSTGRES_IP}"
                    export TABLE_NAME="{ONLINE_TABLE}"
                    export DATABASE="{DB}"

                    source ~/venv/bin/activate
                    python3 ~/data_mainetance/backup_to_gcs.py
                    """.format(PWD=os.environ['SQL_PWD']
                               ,USER=USER
                               ,POSTGRES_IP=POSTGRES_IP
                               ,ONLINE_TABLE=IMOVEIS_TABLE
                               ,DB=DATABASE)


send_sql_to_gcs = SSHOperator(
    default_args=default_args,
    task_id='send_sql_to_gcs',
    ssh_conn_id='ssh_python',
    command=upload_data_to_gcs
)


shell_clean_stage_gcs = """
               gsutil -m rm -rf {stage}
               """.format(stage=STAGE_GCS_FOLDER)



clean_stage_folder_gcs = SSHOperator(
    default_args=default_args,
    task_id='clean_stage_folder_gcs',
    ssh_conn_id='ssh_python',
    command=shell_clean_stage_gcs
)

turn_off_machine = BashOperator(
    bash_command="poweroff"
)

[ingest_new_data, get_offline_urls] >> separate_new_data  
[ingest_new_data, get_offline_urls] >> update_online_table
[ingest_new_data, get_offline_urls] >> clean_stage_tables

[separate_new_data,update_online_table,clean_stage_tables] >> send_sql_to_gcs
    
send_sql_to_gcs >> clean_stage_folder_gcs

clean_stage_folder_gcs >> turn_off_machine 