from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {table} 
        {sql_query};
    """      

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",                 
                 table="",
                 sql_query="",
                 delete_first=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.delete_first = delete_first

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_first:
            self.log.info(f"Deleting data from {self.table} dimension table")
            redshift.run("DELETE FROM {}".format(self.table))
           
        self.log.info(f"Inserting data from fact table into {self.table} dimension table")
        redshift.run(LoadDimensionOperator.insert_sql.format(
            table=self.table,
            sql_query=self.sql_query
        ))
        self.log.info(f"Inserting {self.table} dimension table data completed")
