from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {'owner': 'qwertywert', 'depends_on_past': False, 'start_date': datetime(2020, 1, 1), 'retries': 0}

dag = DAG('report_dag', default_args=default_args, catchup=False, schedule_interval='00 12 * * 1')

t1 = BashOperator(task_id='my_task1', bash_command='python3 script_for_dag.py', dag=dag)


t1