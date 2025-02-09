from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'yerassyl',
}

#Create 4 tasks with simple prints
def task1():
    print('Task 1 is running')

def task2():
    print('Task 2 is running')

def task3():
    print('Task 3 is running')

def task4():
    print('Task 4 is running')



with DAG(
    dag_id='execute_python_operator',
    description='Python operator in the DAG',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['dependencies', 'python'],
) as dag:
    task1 = PythonOperator(
        task_id='task1',
        python_callable=task1,
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=task2,
    )

    task3 = PythonOperator(
        task_id='task3',
        python_callable=task3,
    )

    task4 = PythonOperator(
        task_id='task4',
        python_callable=task4,
    )


task1 >> [task2, task3]
[task2, task3] >> task4 