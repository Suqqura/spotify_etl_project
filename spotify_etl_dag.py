from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from spotify_project.scripts.fetch_recent_tracks import fetch_recent_tracks
from spotify_project.scripts.transform_recent_tracks import transform_recent_tracks
from spotify_project.scripts.load_to_database import load_data_to_db
from spotify_project.scripts.fetch_top_tracks import fetch_top_tracks
from spotify_project.scripts.fetch_top_artists import fetch_top_artists

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8),
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

# Define tasks 
fetch_top_tracks_task = PythonOperator(
    task_id='fetch_top_tracks_long_term',
    python_callable=fetch_top_tracks,
    dag=dag,
)

fetch_top_artists_task = PythonOperator(
    task_id='fetch_top_artists_long_term',
    python_callable=fetch_top_artists,
    dag=dag,
)

fetch_recent_task = PythonOperator(
    task_id='fetch_recent_tracks',
    python_callable=fetch_recent_tracks,
    dag=dag,
)

transform_recent_task = PythonOperator(
    task_id='transform_recent_tracks',
    python_callable=transform_recent_tracks,
    dag=dag,
)

load_recent_task = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_data_to_db,
    dag=dag,
)

# Define task dependencies
fetch_top_tracks_task >> fetch_top_artists_task >> fetch_recent_task >> transform_recent_task >> load_recent_task
