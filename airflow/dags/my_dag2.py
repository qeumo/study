from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'qwertywert',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 0
}

dag = DAG('graph_dag',
    default_args=default_args,
    catchup=False,
    schedule_interval='00 20  * * *')
   

def hello2():
    print('Hello world 2')

def hello3():
    print('Hello world 3')


t1 = BashOperator(
    task_id='my_task1',
    bash_command='python3 hello_world.py',
    dag=dag
    )
    
t2 = PythonOperator(
    task_id='my_task2',
    python_callable=hello2,
    dag=dag
    )

t3 = PythonOperator(
    task_id='my_task3',
    python_callable=hello3,
    dag=dag
    )

t1 >> t2 >> t3