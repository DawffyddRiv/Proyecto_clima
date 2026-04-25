import requests 
import logging
import pandas as pd

class ExtractorDatos:
    def __init__(self,paseAPI):  #En este caso no se requiere el pase
        self.paseAPI=paseAPI
    
    def busqueda(self,fecha_inicio,fecha_fin):
        start_date=fecha_inicio
        end_date=fecha_fin
        #url=f'https://api.open-meteo.com/v1/forecast?latitude=19.43&longitude=-99.13&hourly=temperature_2m,precipitation&past_days=7&forecast_days=0'
        url2=f"https://api.open-meteo.com/v1/forecast?latitude=19.43&longitude=-99.13&hourly=temperature_2m,precipitation&start_date={start_date}&end_date={end_date}"
        try:
            r=requests.get(url2,timeout=30)
            r.raise_for_status()
            
        except requests.exceptions.HTTPError as e:
            logging.error(f"Error web-http: {e}")
        except requests.exceptions.Timeout as e:
            logging.error(f"El tiempo para recibir respuesta se agotó {e}")
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Error de conexion {e}")
        except ValueError as e:
            logging.error(f"Error al parsear el archivo JSON {e}")
        
        return r.json()
    
    def extrae_datos(self,resultado):
        try:
            return pd.DataFrame(resultado["hourly"])
        except (TypeError,KeyError) as e: #Si esta vacio (None)
            logging.warning(f"Se realizó la extracción sin datos o la llave hourly es incorrecta: {e}")
            return pd.DataFrame()



    #def extrae_datos(self,resultado):
    #    data=[]
    #    for dato in resultado:
    #        registro=self