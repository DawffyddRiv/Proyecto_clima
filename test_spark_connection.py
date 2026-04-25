from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id='prueba_conexion_spark',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_spark = SparkSubmitOperator(
        task_id='verificar_version_spark',
        conn_id='spark_default',
        application='/opt/airflow/spark_scripts/test_script.py', # Lo crearemos en el siguiente paso
        total_executor_cores=1,
        executor_memory='512m',
        conf={'spark.master': 'spark://spark-master:7077'}
    )
