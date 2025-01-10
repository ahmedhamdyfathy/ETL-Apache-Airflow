from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the default_args dictionary
default_args = {
    'owner': 'user',  # The owner of the DAG
    'start_date': datetime(2025, 1, 1),  # When the DAG should start
    'retries': 1,  # How many times to retry the task on failure
}

# Instantiate the DAG
with DAG(
    'multiple_task_dag',  # The name of the DAG
    default_args=default_args,  # Default arguments for the DAG
    description='DAG with multiple tasks and dependencies',
    schedule_interval=None,  # Set to None to run only manually
    catchup=False,  # Disable catchup for simplicity
    template_searchpath='/home/user1994/airflow/dags/bash_script'
) as dag:

    # Task A: Print "Starting Task A"
    task_a = BashOperator(
        task_id='task_a',
        bash_command='task_A.sh'
    )

    # Task B: Print "Starting Task B"
    task_b = BashOperator(
        task_id='task_b',
        bash_command='task_B.sh'
    )

    # Task C: Print "Task C Completed"
    task_c = BashOperator(
        task_id='task_c',
        bash_command='task_C.sh'
    )
    
    task_d = BashOperator(
      task_id = 'task_d',
      bash_command='task_D.sh'
    )
    
    task_e = BashOperator(
      task_id = 'task_e',
      bash_command='task_E.sh'
    )
    
    task_f = BashOperator(
      task_id = 'task_f',
      bash_command='task_F.sh'
    )
    
    task_g = BashOperator(
      task_id = 'task_g',
      bash_command='task_G.sh'
    )

    # Set task dependencies
    task_a >> task_b >> task_e # Task B depends on Task A
    task_a >> task_c >> task_f  # Task C depends on Task B
    task_a >> task_d >> task_g 