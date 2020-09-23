from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id,
        self.checks = checks

    def execute(self, context):
        # get redshift hook 
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        errors = []

        # Verify if queries are valid
        for check in self.checks:
            query = check['check_sql']
            expectation = check['expected_result']
            result = redshift.get_records(query)

            if result[0][0] != expectation:
                errors.append(query)

        if errors:
            self.log.info('Tests failed.')
            self.log(errors)
            raise ValueError(f'Data quality check failed.')   
    