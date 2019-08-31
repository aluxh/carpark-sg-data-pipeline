import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DailyFactsCalculatorOperator(BaseOperator):
    """
    Facts Calculator by day Operator

    The function will take in the params configured in the dag, and run the SQL statement to calculate
    the MAX, MIN and AVG of the data on a daily basis

    Args:
        redshift_conn_id: Configuration to connect to Redshift.
        origin_table: The table where summarize SQL statement will be ran on.
        destination_table: The table where results will be saved into.
        fact_column: The column in the origin_table to run the calculation
        groupby_column: The column to be used for group calculation.

    """
    ui_color = '#5934eb'
    # SQL statement template, grouping by month and day to summarise the data daily for every month.
    facts_sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    SELECT
        t.month, t.day, {groupby_column},
        MAX({fact_column}) AS max_{fact_column},
        MIN({fact_column}) AS min_{fact_column},
        AVG({fact_column}) AS average_{fact_column}
    FROM {origin_table}
    LEFT JOIN time AS t
    ON t.date_time = {origin_table}.date_time
    GROUP BY t.month, t.day, {groupby_column};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 origin_table="",
                 destination_table="",
                 fact_column="",
                 groupby_column="",
                 *args, **kwargs):

        super(DailyFactsCalculatorOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.origin_table = origin_table
        self.destination_table = destination_table
        self.fact_column = fact_column
        self.groupby_column = groupby_column
        

    def execute(self, context):
        # Fetch the redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Use the `facts_sql_template` and run the query against redshift
        self.log.info("Running the facts table calculations")
        formatted_sql = DailyFactsCalculatorOperator.facts_sql_template.format(
            destination_table = self.destination_table,
            groupby_column = self.groupby_column,
            fact_column = self.fact_column,
            origin_table = self.origin_table
        )
               
        redshift.run(formatted_sql)
        
