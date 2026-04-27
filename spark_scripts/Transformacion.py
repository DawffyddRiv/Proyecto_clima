from Extraccion import ExtractorDatos
import logging
import pandas as pd

class Transformador:
    def __init__(self,datafra):
        #self.df=datafra.copy()
        self.ruta=datafra

    def ajustardf(self,hora_ini,hora_fin):
        try:
            self.df=pd.read_csv(self.ruta)
            self.df=self.df.rename(columns={"time":"fecha","temperature_2m":"temperatura_c","precipitation":"precipitacion_mm"})
            self.df["fecha"]=pd.to_datetime(self.df['fecha'], errors='coerce')
            self.df=self.df[(self.df["fecha"].dt.hour >= hora_ini) & (self.df["fecha"].dt.hour <=hora_fin)]            
            #return self.df   <- En caso de que requieras el dataframe para pasarlo como parametro  
            self.df.to_csv("datos_clima_export.csv", index=False)
            logging.info("Se ha exportado el DataFrame a el archivo csv")

            
            nulosT=len(self.df[self.df["temperatura_c"].isna()])
            nulosP=len(self.df[self.df["precipitacion_mm"].isna()])

            negativosT=len(self.df[self.df["temperatura_c"] < 0 ])
            negativosP=len(self.df[self.df["precipitacion_mm"] <0 ])
            print(f"Los registros nulos de Temperatura son {nulosT} y los nulos de precipitacion son {nulosP}")
            print(f"los registros negativos de Temperatura son:{negativosT} y los negativos para precipitacion son {negativosP} ")
            print(self.df.head())
            print(self.df.info())
        except KeyError as e:
            logging.error(f"Error. Puede hacer falta una columna {e}")
        except Exception as e:
            logging.error(f"Error. Existe un fallo en transformación de datos {e}")


#df.to_csv("datos_clima_export.csv", index=False)