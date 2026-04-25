from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hola David, Airflow está funcionando")

with DAG(
    dag_id='dag_de_prueba_minimalista',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:

    tarea_1 = PythonOperator(
        task_id='saludo',
        python_callable=hello_world
    )
