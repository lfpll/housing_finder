source venv/bin/activate
export SQL_PWD=luiz28
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@127.0.0.1/airflow
airflow initdb
screen -d -m airflow scheduler
screen airflow webserver -p 5000