from datetime import datetime
from datetime import timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
#Here, we should set start_date to a constant date.
    'start_date': datetime(2020, 7, 3),
    'end_date': datetime(2020, 7, 4),
    'depends_on_past':False,
    'retries': 3,
    'retry_delay':timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          catchup=False,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )
#######################################################################
#                                                                     #
# For the next time always remember set connection in UI first!!!!!!! #
#                                                                     #
#######################################################################

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables=PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table_name='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    redshift_id='redshift',
    credentials='aws_credentials',
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table_name='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    redshift_id='redshift',
    credentials='aws_credentials',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_id='redshift',
    credentials='aws_credentials',
    sql=SqlQueries.songplay_table_insert,
    mode='insert'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# start_operator >> create_tables
# create_tables >> stage_events_to_redshift
# create_tables >> stage_songs_to_redshift
# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator