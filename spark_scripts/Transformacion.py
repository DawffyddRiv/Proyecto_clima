from Extraccion import ExtractorDatos

import pandas as pd

class Transformador:
    def __init__(self,datafra):
        self.df=datafra.copy()
    def ajustardf(self,hora_ini,hora_fin):
        self.df=self.df.rename(columns={"time":"fecha","temperature_2m":"temperatura_c","precipitation":"precipitacion_mm"})
        self.df["fecha"]=pd.to_datetime(self.df['fecha'], errors='coerce')
        self.df=self.df[(self.df["fecha"].dt.hour >= hora_ini) & (self.df["fecha"].dt.hour <=hora_fin)]
        return self.df


extractor=ExtractorDatos(paseAPI=None)
informacion=extractor.busqueda("2026-04-17","2026-04-23")
dat=extractor.extrae_datos(informacion)
dfa=Transformador(dat)
df=dfa.ajustardf(6,22)

nulosT=len(df[df["temperatura_c"].isna()])
nulosP=len(df[df["precipitacion_mm"].isna()])

negativosT=len(df[df["temperatura_c"] < 0 ])
negativosP=len(df[df["precipitacion_mm"] <0 ])
print(f"Los registros nulos de Temperatura son {nulosT} y los nulos de precipitacion son {nulosP}")
print(f"los registros negativos de Temperatura son:{negativosT} y los negativos para precipitacion son {negativosP} ")
print(df.head())
print(df.info())
df.to_csv("datos_clima_export.csv", index=False)