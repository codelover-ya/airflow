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
    tags=['tag1', 'tag2']
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='''
            echo "Task 1 Started"

            for i in {1..10}
            do
                echo "Task 1: $i"
            done

            echo "Task 1 Finished"
        ''',
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='''
            echo "Task 2 Started"

            sleep 4

            echo "Task 2 Finished"
        ''',
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='''
            echo "Task 3 Started"

            sleep 15

            echo "Task 3 Finished"
        ''',
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command='echo "Task 4 Finished"', 
    )

task1 >> [task2, task3]

task4 << [task2, task3]