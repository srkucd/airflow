from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    import_s3_song="""COPY {table_name} FROM '{s3_link}'
                 CREDENTIALS 'aws_iam_role=arn:aws:iam::045574897248:role/myRedshiftRole'
                 REGION 'us-west-2'
                 TIMEFORMAT 'epochmilisecs'
                 FORMAT AS JSON 'auto'"""
    
    import_s3_event="""COPY {table_name} FROM '{s3_link}'
                       CREDENTIALS 'aws_iam_role=arn:aws:iam::045574897248:role/myRedshiftRole'
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
#This part of function returns error indicator: execute() got an unexpected keyword argument 'context'
#Is the 'context' parameter mandatory?
#What if I just want to use s3 link as indicator?
    def execute(self, s3_link):
        aws_hook=AwsHook(self.credentials)
        credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=redshift_id)
        self.log.info("Clearing.")
        redshift.run("DELETE FROM {table_name}").format(self.table_name)
        
        self.log.info("Importing.")
        if s3_link == 's3://udacity-dend/log_data':
            sql=StageToRedshiftOperator.import_s3_event.format(
                                        table_name=self.table_name,
                                        s3_link=self.s3_link)
            redshift.run(sql)
        elif s3_link == 's3://udacity-dend/song_data':
            sql=StageToRedshiftOperator.import_s3_song.format(
                                        table_name=self.table_name,
                                        s3_link=self.s3_link)
            redshift.run(sql)