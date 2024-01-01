import logging
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define default arguments for the DAG
args = {
    "owner": "Airflow",  # Owner of the DAG
    "start_date": airflow.utils.dates.days_ago(2)  # Start date for the DAG
}

# Create a DAG with a unique identifier ("dag_id") and default arguments
dag = DAG(
    dag_id="snowflake_automation_dag",  # Unique DAG identifier
    default_args=args,  # Default arguments defined above
    schedule_interval=None  # Set the schedule interval (None for manual triggering)
)

# Snowflake SQL queries to create a table and insert data
snowflake_query = [
    """
    # SQL query to create a table if it doesn't exist
    create  table if not exists source_table(
        emp_no int,
        emp_name text,
        salary int,
        hra int,
        Dept text
    );
    """,
    """
    # SQL query to insert data into the 'source_table'
    INSERT INTO source_table VALUES
    (100, 'A', 2000, 100, 'HR'),
    (101, 'B', 5000, 300, 'HR'),
    (102, 'C', 6000, 400, 'Sales'),
    (103, 'D', 500, 50, 'Sales'),
    (104, 'E', 15000, 3000, 'Tech'),
    (105, 'F', 150000, 20050, 'Tech'),
    (105, 'F', 150000, 20060, 'Tech');
    """
]

# Define the DAG structure and tasks
with dag:
    # Task to create the Snowflake table
    create_table = SnowflakeOperator(
        task_id="create_table",  # Unique task identifier
        sql=snowflake_query[0],  # SQL query to execute
        snowflake_conn_id="snowflake_conn"  # Snowflake connection ID
    )
    
    # Task to insert data into the Snowflake table
    insert_data = SnowflakeOperator(
        task_id="insert_snowflake_data",  # Unique task identifier
        sql=snowflake_query[1],  # SQL query to execute
        snowflake_conn_id="snowflake_conn"  # Snowflake connection ID
    )

# Set up the task dependencies
create_table >> insert_data  # 'insert_data' task depends on 'create_table' task
