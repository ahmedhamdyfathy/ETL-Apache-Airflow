import pandas as pd
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Define the default_args dictionary
default_args = {
    'owner': 'user',  # The owner of the DAG
    'start_date': datetime(2025, 1, 1),  # When the DAG should start
    'retries': 1,  # How many times to retry the task on failure
}

def read_csv_file():
  df = pd.read_csv('/home/user1994/airflow/dags/datasets/insurance.csv')
  print(df)
  return df.to_json()


def remove_null_values(**kwagrs):
  ti = kwagrs['ti']
  json_data = ti.xcom_pull(task_ids='read_csv_file')
  df = pd.read_json(json_data)
  df = df.dropna()
  print(df)
  return df.to_json()
  
  
def groupby_smoker(ti):
  json_data = ti.xcom_pull(task_ids='remove_null_values')
  df = pd.read_json(json_data)
  smoker_df = df.groupby('smoker').agg({
    'age': 'mean', 
    'bmi': 'mean',
    'charges': 'mean'
  }).reset_index()
  
  smoker_df.to_csv('/home/user1994/airflow/output/grouped_by_smoker.csv', index=False)
  
  
def groupby_region(ti):
  json_data = ti.xcom_pull(task_ids='remove_null_values')
  df = pd.read_json(json_data)
  region_df = df.groupby('region').agg({
      'age': 'mean', 
      'bmi': 'mean', 
      'charges': 'mean'
  }).reset_index()
    
  region_df.to_csv('/home/user1994/airflow/output/grouped_by_region.csv', index=False)

  

# Instantiate the DAG
with DAG(
    'python_pipeline',  # The name of the DAG
    default_args=default_args,  # Default arguments for the DAG
    description='Running a Python pipeline',  # Description of the DAG
    schedule_interval='@once',  # Set to None to run only manually (or set a cron schedule)
    catchup=False,  # If True, it will backfill the DAG (set False for simplicity)
    tags = ['python', 'transform', 'pipeline']
)as dag:

# Create a PythonOperator to execute the 'hello_world' function
  read_csv_file = PythonOperator(
      task_id='read_csv_file',  # The task ID
      python_callable= read_csv_file,  # The Python function to execute
      #dag=dag  # The DAG to associate this task with
  )
  
  remove_null_values = PythonOperator(
    task_id ='remove_null_values',
    python_callable=remove_null_values,
  )
  
  groupby_smoker = PythonOperator(
    task_id='groupby_smoker',
    python_callable = groupby_smoker
  )
  
  groupby_region = PythonOperator(
    task_id='groupby_region',
    python_callable=groupby_region
  )

read_csv_file >> remove_null_values >> [ groupby_smoker , groupby_region]