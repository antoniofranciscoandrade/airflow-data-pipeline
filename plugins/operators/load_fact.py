from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 table='',
                 sql = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql
        

    def execute(self, context):
        
        # get redshift hook 
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        # construct and run load query
        query = self.load_query.format( 
                                        self.table,
                                        self.sql
                                      )
        
        self.log.info(f"Loading data to {self.table}")
        redshift.run(query)
        
        self.log.info('LoadFactOperator finished.')
        
        
      
