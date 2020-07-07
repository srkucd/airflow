from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_id='',
                 sql='',
                 mode='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_id=redshift_id
        self.sql=sql
        self.mode=mode

    def execute(self, context):
        self.log.info('Setting.')
        redshift=PostgresHook(postgres_conn_id=self.redshift_id)
        
        self.log.info('Loading dimension table.')
        if self.mode == 'insert':
            redshift.run("INSERT INTO songplays " + self.sql)
        else:
            redshift.run('DELETE FROM songplays')
            redshift.run("INSERT INTO songplays " + self.sql)
