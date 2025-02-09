from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'yerassyl',
}

#Create 4 tasks with simple prints
def increment_by_one(number: int):
    print(f"Number is {number}")
    return number + 1

def multiply_by_two(ti):
    number = ti.xcom_pull(task_ids='increment_by_one')
    print(f"Number is {number}")
    return number * 2  

def subtract_by_three(ti):
    number = ti.xcom_pull(task_ids='multiply_by_two')
    print(f"Number is {number}")
    return number - 3

def print_number(ti): 
    number = ti.xcom_pull(task_ids='subtract_by_three')
    print(f"Number is {number}")



with DAG(
    dag_id='cross_task_communication',
    description='Cross task communication',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['xcom', 'python'],
) as dag:
    
    increment_by_one = PythonOperator(
        task_id='increment_by_one',
        python_callable=increment_by_one,
        op_args=[1],
    )

    multiply_by_two = PythonOperator(
        task_id='multiply_by_two',
        python_callable=multiply_by_two,
    )

    subtract_by_three = PythonOperator(
        task_id='subtract_by_three',
        python_callable=subtract_by_three,
    )

    print_number = PythonOperator(
        task_id='print_number',
        python_callable=print_number,
    )

increment_by_one >> multiply_by_two >> subtract_by_three >> print_number