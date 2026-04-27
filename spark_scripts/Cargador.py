from Extraccion import ExtractorDatos
from Transformacion import Transformador
import pandas as pd
import sqlite3
import logging

class CargaDatos:
    def __init__(self,archivo_clima="clima.db"):
        self.archivo_clima=archivo_clima
        self.conn=sqlite3.connect(archivo_clima)#Si el archivo clima.db existe, se abre la bd si no existe, se crea a través de la conexión.

    def cargar_csv(self,archivo_csv,nombre_tabla="mi_clima"):
        try:
            df=pd.read_csv(archivo_csv)
            df.to_sql(nombre_tabla,self.conn,if_exists="replace", index=False)
            logging.warning(f"Se cargaron los datos a la tabla: {nombre_tabla} de {self.archivo_clima} ") 
        except Exception as e:
            logging.error(f"Error al cargar el archivo csv a sqlite: {e}")
    
    def consultando(self, consulta):
        try:
            respuesta=pd.read_sql_query(consulta,self.conn)
            return respuesta
        except Exception as e:
            logging.error(f"Error en la consulta: {e}")
            return None
    def cerrar_conexion(self):
        logging.info(f"Se cerró la conexion a {self.archivo_clima}")

#Extraccion
extractor=ExtractorDatos(paseAPI=None)
informacion=extractor.busqueda("2026-04-17","2026-04-23")
dat=extractor.extrae_datos(informacion)
#Transformación
dfa=Transformador(dat)
df=dfa.ajustardf(6,22)
#Carga
objcarga=CargaDatos()
objcarga.cargar_csv("datos_clima_export.csv")
#Consulta A — Temperatura promedio por día
consulta_uno=objcarga.consultando("Select strftime('%Y-%m-%d',fecha) as dia , avg(temperatura_c) as Temp_Promedio from mi_clima group by dia order by fecha asc") #Aqui lo primero que se me ocurre es un group by para la primer consulta
print(consulta_uno)
#Consulta B — Horas con precipitación
consulta_dos=objcarga.consultando("select fecha,precipitacion_mm from mi_clima where precipitacion_mm > 0")
print(consulta_dos)
#Consulta C — Día con mayor variación térmica
consulta_3=objcarga.consultando("select date(fecha) as Fecha_, (max(temperatura_c)-min(temperatura_c)) as DiferenciaTemp from mi_clima group by Fecha_ order by DiferenciaTemp desc limit 1")
print(consulta_3)#Aqui el detalle fue la fecha la cual al tener la hora dentro de su formato me hacia considerar cada registro como único lo que impedia el group by.
#Consulta D — Resumen diario
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
