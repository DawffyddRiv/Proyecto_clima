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


extractor=ExtractorDatos(paseAPI=None)
informacion=extractor.busqueda("2026-04-17","2026-04-23")
dat=extractor.extrae_datos(informacion)
dfa=Transformador(dat)
df=dfa.ajustardf(6,22)

objcarga=CargaDatos()

objcarga.cargar_csv("datos_clima_export.csv")
consulta_uno=objcarga.consultando("Select strftime('%Y-%m-%d',fecha) as dia , avg(temperatura_c) as Temp_Promedio from mi_clima group by dia order by fecha asc") #Aqui lo primero que se me ocurre es un group by para la primer consulta
print(consulta_uno)
consulta_dos=objcarga.consultando("select fecha,precipitacion_mm from mi_clima where precipitacion_mm > 0")
print(consulta_dos)

objcarga.cerrar_conexion()
