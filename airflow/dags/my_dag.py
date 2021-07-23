from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'qwertywert',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 0
}

dag = DAG('my_dag1',
    default_args=default_args,
    catchup=False,
    schedule_interval='00 20  * * *')
   

def hello1():
    print('Hello world 1')

def hello2():
    print('Hello world 2')


t1 = PythonOperator(
    task_id='my_task1',
    python_callable=hello1,
    dag=dag
    )
    
t2 = PythonOperator(
    task_id='my_task2',
    python_callable=hello2,
    dag=dag
    )
    
t1 >> t2