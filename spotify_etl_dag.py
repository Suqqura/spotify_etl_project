from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/path_to_your_airflow_home/dags/spotify_project/scripts')

from fetch_spotify_data import fetch_recent_tracks
from transform_spotify_data import transform_spotify_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spotify_etl',
    default_args=default_args,
    description='Spotify ETL process',
    schedule_interval='@daily',
)

fetch_task = PythonOperator(
    task_id='fetch_spotify_data',
    python_callable=fetch_recent_tracks,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_spotify_data',
    python_callable=transform_spotify_data,
    dag=dag,
)

fetch_task >> transform_task
