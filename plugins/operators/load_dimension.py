from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 table='',
                 sql = '',
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.table=table
        self.truncate = truncate
        self.sql_prefix = ''
        self.sql = sql

    def execute(self, context):
        # get redshift hook 
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        
        if self.truncate:
            sql_prefix = 'TRUNCATE TABLE {} '.format(self.table)

        load_query = sql_prefix + 'INSERT INTO {} {}'(self.table, self.sql)
        
        # construct and run load query
        query = self.load_query.format(load_query)
                                       
        self.log.info(f"Loading data to {self.table}")
        redshift.run(query)
        
        self.log.info('LoadDimensionOperator finished')
