from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago

# Define the default_args dictionary
default_args = {
    'owner': 'user',  # The owner of the DAG
    'start_date': datetime(2025, 1, 1),  # When the DAG should start
    'retries': 1,  # How many times to retry the task on failure
}

def print_function():
  print("the simplest possible operator python!")

# passing parameters to python callables
def greet_hello(name):
  print("hello, {name}!".format(name=name))
  
def greet_hello_with_city(name, city):
  print("hello, {name} from {city}".format(name=name, city=city))
  
# Instantiate the DAG
with DAG(
    'python_operator',  # The name of the DAG
    default_args=default_args,  # Default arguments for the DAG
    description='A simple Hello World Airflow DAG',  # Description of the DAG
    schedule_interval=None,  # Set to None to run only manually (or set a cron schedule)
    catchup=False,  # If True, it will backfill the DAG (set False for simplicity)
)as dag:

# Create a PythonOperator to execute the 'hello_world' function
  task_A = PythonOperator(
      task_id='hello_task',  # The task ID
      python_callable=print_function,  # The Python function to execute
      #dag=dag  # The DAG to associate this task with
  )
  
  task_b = PythonOperator(
    task_id = 'greet_hello',
    python_callable=greet_hello,
    op_kwargs={'name':'ahmed'}
  )
  
  task_C = PythonOperator(
    task_id = 'greet_hello_with_city',
    python_callable=greet_hello_with_city,
    op_kwargs={'name':'ahmed',
               'city':'sadat city'
               }
  )

# Set the task to run (in this case, there is only one task, so no dependencies)
task_A
task_b
task_C
