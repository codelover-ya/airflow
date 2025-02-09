import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'yerassyl',
}

def read_csv_file():
    df = pd.read_csv('/Users/erasylabenov/airflow/dags/datasets/insurance.csv')
    print(df)
    return df.to_json()

def remove_null_values(**kwargs):
    ti = kwargs['ti']
    df = pd.read_json(ti.xcom_pull(task_ids='read_csv_file'))
    df = df.dropna()
    print(df)
    return df.to_json()

def group_by_smoker(ti):
    df = pd.read_json(ti.xcom_pull(task_ids='remove_null_values'))

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
        }).reset_index()

    smoker_df = smoker_df.to_csv('/Users/erasylabenov/airflow/dags/output/group_by_smoker.csv', index=False)

def group_by_region(ti):
    df = pd.read_json(ti.xcom_pull(task_ids='remove_null_values'))

    region_df = df.groupby('region').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean'
        }).reset_index()

    region_df = region_df.to_csv('/Users/erasylabenov/airflow/dags/output/group_by_region.csv', index=False)


with DAG(
    dag_id='cross_task_communication',
    description='Cross task communication',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['python', 'transform', 'pipeline'],
) as dag: 

    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file,
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values,
    )

    group_by_smoker = PythonOperator(
        task_id='group_by_smoker',
        python_callable=group_by_smoker,
    )

    group_by_region = PythonOperator(
        task_id='group_by_region',
        python_callable=group_by_region,
    )

read_csv_file >> remove_null_values >> [group_by_smoker, group_by_region]
    