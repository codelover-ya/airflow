from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'yerassyl',
}

with DAG(
    dag_id='execute_multiple_tasks',
    description='DAG with multiple tasks and dependencies',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['scripts', 'template_search'],
    template_searchpath='/Users/erasylabenov/airflow/dags/bash_scripts'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='task1.sh'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='task2.sh'
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='task3.sh'
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command='task4.sh'
    )

    task5 = BashOperator(
        task_id='task5',
        bash_command='task5.sh'
    )

    task6 = BashOperator(
        task_id='task6',
        bash_command='task6.sh'
    )

    task7 = BashOperator(
        task_id='task7',
        bash_command='task7.sh'
    )

task1 >> task2 >> task5
task1 >> task3 >> task6
task1 >> task4 >> task7