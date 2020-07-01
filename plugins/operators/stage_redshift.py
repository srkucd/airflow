from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    import_s3_song="""COPY {table_name} FROM '{s3_link}'
                 ACCESS_KEY_ID '{key_id}'
                 SECRET_ACCESS_KEY '{secret_id}'
                 REGION 'us-west-2'
                 TIMEFORMAT 'epochmilisecs'
                 FORMAT AS JSON 'auto'"""
    
    import_s3_event="""COPY {table_name} FROM '{s3_link}'
                       ACCESS_KEY_ID '{key_id}'
                       SECRET_ACCESS_KEY '{secret_id}'
                       TIMEFORMAT as 'epochmillisecs'
                       TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                       REGION 'us-west-2'
                       FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"""

    @apply_defaults
    def __init__(self,
                 table_name='',
                 s3_link='',
                 redshift_id='',
                 credentials='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name=table_name
        self.s3_link=s3_link
        self.redshift_id=redshift_id
        self.credentials=credentials

    def execute(self, context):
        aws_hook=AwsHook(self.credentials)
        credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_id)
        s3_link=context
        self.log.info("Clearing & Restoring.")
        redshift.run("DELETE FROM {}".format(self.table_name))

        #The commented condition is used for final version.
        self.log.info("Importing.")
        if 's3://udacity-dend/log_data' in s3_link:
            formatted_sql=StageToRedshiftOperator.import_s3_event.format(
                                        table_name=self.table_name,
                                        s3_link=self.s3_link,
                                        key_id=credentials.access_key, 
                                        secret_id=credentials.secret_key)
            redshift.run(formatted_sql)
            self.log.info("Importing log data complete")
        elif 's3://udacity-dend/song_data' in s3_link:
            formatted_sql=StageToRedshiftOperator.import_s3_song.format(
                                        table_name=self.table_name,
                                        s3_link=self.s3_link,
                                        key_id=credentials.access_key, 
                                        secret_id=credentials.secret_key)
            redshift.run(formatted_sql)
            self.log.info("Importing song data complete")
        else:
            self.log.info('Unknown s3 link - {}'.format(s3_link))