from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'yerassyl',
}

#Create 4 tasks with simple prints
def task1(name: str):
    print(f"Task 1 is running with name: {name}")

def task2(name: str, age: int):
    print(f"Task 2 is running with name: {name} and age: {age}")




with DAG(
    dag_id='execute_python_operator',
    description='Python operator in the DAG',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['dependencies', 'python'],
) as dag:
    task1 = PythonOperator(
        task_id='task1_with_name',
        python_callable=task1,
        op_kwargs={'name': 'Yerassyl'},
    )

    task2 = PythonOperator(
        task_id='task2_with_name_and_age',
        python_callable=task2,
        op_kwargs={'name': 'Yerassyl', 'age': 20},
    )



task1 >> task2 