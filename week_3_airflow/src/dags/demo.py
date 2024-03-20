from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
import pandas as pd
import requests

# A DAG represents a workflow, a collection of tasks
with DAG(
    dag_id="demo",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule_interval="0 0 * * *") as dag:
    # Tasks are represented as operators

    def say_hello():
        print("Deneme")


    def convert_json_to_dataframe():
        url = "https://microsoftedge.github.io/Demos/json-dummy-data/64KB.json"
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data)
        print(df)


    dag_start = DummyOperator(task_id="start")

    dag_final = DummyOperator(task_id="final")
    task2 = DummyOperator(task_id="task2")

    hello = BashOperator(task_id="hello", bash_command="echo hello")

    py_operator = PythonOperator(task_id="python_task", python_callable=say_hello, dag=dag)

    download_json_file = PythonOperator(task_id="json_file_task", python_callable=convert_json_to_dataframe, dag=dag)
  




    # Set dependencies between tasks
    #dag_start >> [hello, py_operator] >> dag_final

    dag_start >> [hello,py_operator]
    py_operator >> download_json_file >> dag_final
    hello >> task2 >> dag_final