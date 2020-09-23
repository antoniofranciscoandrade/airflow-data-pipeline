from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    

    @apply_defaults
    def __init__(self,
                 conn_id='',
                 aws_credentials = '',
                 s3_path='',
                 format_path='auto',
                 table='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credentials = aws_credentials
        self.s3_path = s3_path
        self.format_path = format_path
        self.table = table
        self.copy_query = """
                                COPY {}
                                FROM '{}'
                                ACCESS_KEY_ID '{}'
                                SECRET_ACCESS_KEY '{}'
                                JSON '{}'
                                TIMEFORMAT AS 'epochmillisecs'
                                region 'us-west-2'
                            """
        

    def execute(self, context):
        
        # get redshift hook and aws credentials
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        aws = AwsHook(self.aws_credentials)
        credentials = aws.get_credentials()
        
        # drop staging table if it exists
        self.log.info(f'Deleting {self.table} if it exists.')
        redshift.run(f'DROP TABLE IF EXISTS {self.table}')
        
      
        # construct and run copy query
        query = self.copy_query.format(self.table, 
                                       self.s3_path,
                                       credentials.access_key,
                                       credentials.secret_key,
                                       self.format_path
                                      )
                                       
                                       
        self.log.info(f"Copying data from {self.s3_path} to {self.table}")
        redshift.run(query)
        
        self.log.info('StageToRedshiftOperator finished.')
        




