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

# def increment_by_1(counter):
#   print("counter {counter}".format(counter=counter))
#   return counter + 1

# def mltiply_by_100(counter):
#   print("counter {counter}".format(counter=counter))
#   return counter * 100

def increment_by_1(value):
  print("value {value}".format(value=value))
  return value + 1

def mltiply_by_100(ti):
  value = ti.xcom_pull(task_ids='increment_by_1')
  print('value {value}!'.format(value=value))
  return value * 100

def substract_9(ti):
  value = ti.xcom_pull(task_ids = 'mltiply_by_100')
  print('value {value}!'.format(value=value))
  return value - 9

def print_value(ti):
  value = ti.xcom_pull(task_ids = 'substract_9')
  print('value {value}!'.format(value=value))


with DAG(
  'cross_task_communication',  # The name of the DAG
    default_args=default_args,  # Default arguments for the DAG
    description='cross task communication with Xcom',  # Description of the DAG
    schedule_interval='@daily',  # Set to None to run only manually (or set a cron schedule)
    catchup=False,  # If True, it will backfill the DAG (set False for simplicity)
    tags=['Xcom','python']
)as dag:
  task_A = PythonOperator(
    task_id = 'increment_by_1',
    python_callable=increment_by_1,
    op_kwargs={'value':1}
  )
  
  task_b = PythonOperator(
    task_id = 'mltiply_by_100',
    python_callable= mltiply_by_100
    
  )
  
  task_c = PythonOperator(
    task_id = 'substract_9',
    python_callable= substract_9
  )
  
  task_d = PythonOperator(
    task_id = 'print_value',
    python_callable= print_value
  )
  
  
  task_A >> task_b >> task_c >> task_d
