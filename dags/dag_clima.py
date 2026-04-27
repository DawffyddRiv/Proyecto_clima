from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
#from airflow import DAG 
sys.path.append('/opt/airflow')

from spark_scripts.Cargador import ExtractorDatos
from spark_scripts.Transformacion import Transformador
from spark_scripts.Cargador import CargaDatos

def tarea_extraccion():
    extractor=ExtractorDatos(paseAPI=None)
    informacion=extractor.busqueda("2026-04-17","2026-04-23")
    dat=extractor.extrae_datos(informacion)
    return dat

def tarea_transformacion(**context):#Parte del último cambio. 
    ruta = context['ti'].xcom_pull(task_ids='extraccion') #Parte del último cambio.Desde esta instancia,recupera del buzón(XCom) la ruta que guardé en la etapa extraccion.
    dfa=Transformador(ruta)
    df=dfa.ajustardf(6,22)   


def tarea_carga():
    objcarga=CargaDatos()
    objcarga.cargar_csv("/opt/airflow/datos_clima_export.csv")#<---- aqui debe ir la ruta
    #Consultas solicitadas
    consulta_uno=objcarga.consultando("Select strftime('%Y-%m-%d',fecha) as dia , avg(temperatura_c) as Temp_Promedio from mi_clima group by dia order by fecha asc") #Aqui lo primero que se me ocurre es un group by para la primer consulta
    print(consulta_uno)
    consulta_dos=objcarga.consultando("select fecha,precipitacion_mm from mi_clima where precipitacion_mm > 0")
    print(consulta_dos)
    consulta_3=objcarga.consultando("select date(fecha) as Fecha_, (max(temperatura_c)-min(temperatura_c)) as DiferenciaTemp from mi_clima group by Fecha_ order by DiferenciaTemp desc limit 1")
    print(consulta_3)#Aqui el detalle fue la fecha la cual al tener la hora dentro de su formato me hacia considerar cada registro como único lo que impedia el group by.

#se plantea el CTE con funcion de ventana para introducirlo posteriormente en la consulta
    queryCTE=""" With agrupados as(
    select date(fecha) as Fecha_,
    min(temperatura_c) as tempMin, 
    max(temperatura_c) as tempMax,
    avg(temperatura_c) as tempProm,
    sum(precipitacion_mm) as lluvia_dia from mi_clima group by date(fecha)
    )select Fecha_,tempMin,tempMax,tempProm, 
    sum(lluvia_dia) over (order by Fecha_ rows between unbounded preceding and current row) as lluvia_acumulada from agrupados"""
    consulta_4=objcarga.consultando(queryCTE)
    print(consulta_4)
    objcarga.cerrar_conexion()

#Establecemos el DAG
with DAG(
    dag_id='etl_clima',
    start_date=datetime(2026, 4, 26, 16, 45),  # 23 de octubre a las 15:30 AM
    schedule='@once',
    catchup=False,
    tags=['etl', 'clima open meteo']

) as dag:
    extraccion=PythonOperator(task_id='extraccion',python_callable=tarea_extraccion)
    transformacion=PythonOperator(task_id='transformacion',python_callable=tarea_transformacion)
    carga=PythonOperator(task_id='carga',python_callable=tarea_carga)

#Definiendo el flujo de dependencias
extraccion>>transformacion>>carga
