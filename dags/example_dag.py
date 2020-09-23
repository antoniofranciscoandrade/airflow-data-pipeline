from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'email_on_failure': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id='redshift',
    aws_credentials='aws_credentials',
    s3_path='s3://udacity-dend/log_data',
    format_path='s3://udacity-dend/log_json_path.json',
    table='events_staging'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id='redshift',
    aws_credentials='aws_credentials',
    s3_path='s3://udacity-dend/song_data',
    table='songs_staging'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id = 'redshift',
    table='songplays',
    sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table='users',
    sql = SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table='songs',
    sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table='artists',
    sql = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table='time',
    sql = SqlQueries.time_table_insert
)

dq_checks=[
{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id = 'redshift',
    checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

