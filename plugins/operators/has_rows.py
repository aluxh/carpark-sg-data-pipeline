import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HasRowsOperator(BaseOperator):
    """
    Has Rows Operator

    The function run a SQL statement to test whether the table is empty. 
    It throws an error if the table is empty.

    Args:
        redshift_conn_id: The configuration to connect to the Redshift.
        table: The table name to run the test on.
    """
    ui_color = '#ebe534'
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(HasRowsOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # Fetch the redshift hook
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Run the SQL statement to count the rows in table. And, throw errors if it is empty or no rows.
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        logging.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")

