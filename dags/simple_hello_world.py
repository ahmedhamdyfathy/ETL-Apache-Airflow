from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# Define the default_args dictionary
default_args = {
    'owner': 'user',  # The owner of the DAG
    'start_date': datetime(2025, 1, 1),  # When the DAG should start
    'retries': 1,  # How many times to retry the task on failure
}

# Instantiate the DAG
with DAG(
    'hello_world_bash_dag',  # The name of the DAG
    default_args=default_args,  # Default arguments for the DAG
    description='A simple Hello World Airflow DAG using BashOperator',  # Description of the DAG
    schedule_interval='@daily',  # Set to None to run only manually (or set a cron schedule)
    catchup=False,  # If True, it will backfill the DAG (set False for simplicity)
    tags = ['beginner','bash','hello']
)as dag:

# Create a BashOperator task to print "Hello, World!" using a shell command
  hello_task = BashOperator(
      task_id='hello_task',  # The task ID
      bash_command='echo "Hello, World! again"',  # The shell command to execute
    # The DAG to associate this task with
  )

# Set the task to run (in this case, there is only one task, so no dependencies)
hello_task
