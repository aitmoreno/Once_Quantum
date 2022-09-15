# Databricks notebook source
#!pip install xlrd
#!pip install openpyxl
#!pip install pandas_profiling

# COMMAND ----------

#ETL inicial de los datos
import pandas as pd
import glob
import numpy as np
from pandas_profiling import ProfileReport

# COMMAND ----------

#Definición de la clase principal
#Clase de carga y calidad de los datos
class Once_Quantum_ETL():
  def __init__(self, input_path):
    self.input_path = input_path
    
  def leer_datos_fichero(self):
    self.df_init = pd.read_excel(self.input_path, engine='openpyxl', sheet_name = 'MAESTRO PUNTO VENTA', header = [0,1])
    
  def procesar_fichero(self, fichero):
    df_init = pd.read_excel(fichero, engine='openpyxl', sheet_name = 'MAESTRO PUNTO VENTA', header = [0,1])
    #7 días por fichero
    ndias = 7
    columnaorigen = 12
    output = pd.DataFrame()
    indice = df_init.index
    ficheroresult = pd.DataFrame() 
    for i in range(1, ndias*2):
      #8 columnas por franja
      columnafin = columnaorigen + 8 
      df_temp = df_init.iloc[:,columnaorigen:columnafin]
      franja = df_temp.columns[1][0]
      fecha =  df_temp.columns[2][0]
      CodigoPrevisto = df_temp.iloc[:,0].astype(str)
      inicio = df_temp.iloc[:,2]
      fin = df_temp.iloc[:,3]
      ABS = df_temp.iloc[:,4]
      CodigoSustitucion = df_temp.iloc[:,5].astype(str)
      SegPV = df_temp.iloc[:,7]
      
      columnaorigen = columnafin 
      dict = {'Fichero':fichero, 'Indice': indice, 'Franja': franja, 'Fecha': fecha, 'CodigoPrevisto': CodigoPrevisto, 'Inicio': inicio, 'Fin':fin, 'ABS':ABS, 'CodigoSustitucion':CodigoSustitucion, 'SegPV':SegPV}  
      output = output.append(pd.DataFrame(dict) , ignore_index=True)
    output = output.set_index('Indice')
    ficheroresult = pd.merge(df_init.iloc[:,0:12], output, left_index=True, right_index=True)
    return ficheroresult
    
    
  def procesar_directorio(self):
    path = self.input_path
    self.tablon_inicial = pd.DataFrame() 
    filenames = glob.glob(path + "*.xlsx")
    for file in filenames:
     print("Reading file = ",file)
     self.tablon_inicial =  self.tablon_inicial.append(instancia_carga.procesar_fichero(file) , ignore_index=True) #Acumulamos la información de los ficheros individuales
     self.tablon_inicial = self.tablon_inicial.replace(r'^\s*$', np.nan, regex=True) #Sustituimos espacios en blanco y '' en na
     self.tablon_inicial = self.tablon_inicial.replace(r'nan', np.nan, regex=True) #Sustituimos nan de excel en na de numpy
    #Grabamos el resultado final en formato DBFS
    self.tablon_inicial.to_csv(path + 'Tablon_Inicial.csv') 
    
  def leer_datos_iniciales(self):
    #Cargamos los datos almacenados en el proceso de Ingesta
    self.tablon_inicial = pd.DataFrame() 
    self.tablon_inicial = pd.read_csv('/dbfs/FileStore/tables/ONCE_Quantum/Tablon_Inicial.csv', parse_dates=True)
    #Aseguramos que los Codigos de vendedores sean categóricos
    self.tablon_inicial = self.tablon_inicial.astype({'CodigoPrevisto': 'str', 'CodigoSustitucion': 'str', 'ABS': 'str'})
        
      
  def informe_calidad_dato(self):
    #https://www.pschwan.de/how-to/setting-up-data-quality-reports-with-pandas-in-no-time
    #Summary general
    print(self.tablon_inicial.astype(str).describe())
    
    
    #Calidad del Dato Tablón básico
    data_types = pd.DataFrame(
     self.tablon_inicial.dtypes,
     columns=['Data Type'])
    
    missing_data = pd.DataFrame(
      self.tablon_inicial.isnull().sum(),
      columns=['Missing Values'])
    
    self.dq_report = data_types.join(missing_data)
    
    #Informe completo
    print(self.dq_report)
    self.prof = ProfileReport(self.tablon_inicial.iloc[:,12:21])
    #self.prof.to_file(output_file='/dbfs/FileStore/tables/ONCE_Quantum/informe.html')
    displayHTML(self.prof.to_html()) 
  
  
  
  
  
  

# COMMAND ----------

######################################################################################
# INGESTA DE TODOS LOS FICHEROS 
#####################################################################################

#instancia_carga = Once_Quantum_ETL('/dbfs/FileStore/tables/ONCE_Quantum/')

# COMMAND ----------

#prueba unitaria de un fichero
ficheroresult = instancia_carga.procesar_fichero('/dbfs/FileStore/tables/ONCE_Quantum/GESTIÓN_COBERTURA_NE11__2022_01_03_.xlsx')
ficheroresult[ficheroresult.iloc[:,0] == 2768]

# COMMAND ----------

######################################################################################
#Carga del tablón inicial con los datos de todos los ficheros
######################################################################################

#instancia_carga.procesar_directorio()


# COMMAND ----------

#presentar resultado de la Ingesta inicial
#instancia_carga.tablon_inicial

# COMMAND ----------

###########################################################################################################
#Cargamos los datos almacenados en el proceso de Ingesta, si no queremos reprocesar de nuevo los ficheros
###########################################################################################################
#Inicio de proceso post-ingesta
instancia_datos = Once_Quantum_ETL('/dbfs/FileStore/tables/ONCE_Quantum/')
instancia_datos.leer_datos_iniciales()

# COMMAND ----------

instancia_datos.informe_calidad_dato()

# COMMAND ----------

#Comprobaciones de duplicados

#[instancia_datos.tablon_inicial.groupby(by=[('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código'), 'Franja','Fecha' ], dropna=False).count() > 1] == True
#[instancia_datos.tablon_inicial.pivot_table(index=[('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código'), 'Franja','Fecha' ], aggfunc='size')>1] == True
instancia_datos.tablon_inicial[
  #(instancia_datos.tablon_inicial.iloc[:,1] == 2768)  #& (instancia_datos.tablon_inicial['Franja'] == 'TARDE')   & 
    (instancia_datos.tablon_inicial['Fecha'] == '2022-01-03'  )     ]


# COMMAND ----------

#Conteos de incidencias sin vendedor asignado
print(instancia_datos.tablon_inicial[instancia_datos.tablon_inicial['ABS'].notnull() & instancia_datos.tablon_inicial['CodigoSustitucion'].isna()].count)
print(instancia_datos.tablon_inicial[instancia_datos.tablon_inicial['ABS'].notnull() & instancia_datos.tablon_inicial['CodigoSustitucion'].notnull()].count)
print(instancia_datos.tablon_inicial[instancia_datos.tablon_inicial['ABS'].isna()].count)


# COMMAND ----------

#Hipótesis de nulos por prioridad
df = instancia_datos.tablon_inicial
#pd.options.display.max_rows = 20
pivotado = pd.pivot_table(df[(df['CodigoPrevisto'] == 'nan') | (df['CodigoPrevisto'].isna()) ], index=['SegPV', 'ABS'],  values = ['CodigoPrevisto'], aggfunc=len).merge(df.groupby(['SegPV'])['CodigoPrevisto'].count().to_frame(), left_on='SegPV', right_on='SegPV').rename(columns={"CodigoPrevisto_x": "Codigos No Asigandos", "CodigoPrevisto_y": "Total"})
pivotado['porcentaje_nulos'] = pivotado.iloc[:,0] / pivotado.iloc[:,1]
pivotado['Asignados'] =  pivotado.iloc[:,1] - pivotado.iloc[:,0]
print(pivotado.sum())
print(instancia_datos.tablon_inicial['SegPV'].value_counts().sum())
pivotado
#pd.pivot_table(df[df['CodigoPrevisto'] == 'nan'], index=['SegPV', 'ABS'])
#df.groupby(['SegPV'])['CodigoPrevisto'].count().to_frame()


# COMMAND ----------

pivotado.sum().to_frame()

# COMMAND ----------

instancia_datos.tablon_inicial.replace(r'nan', np.nan, regex=True).astype(str).describe()

# COMMAND ----------

pd.pivot_table(df, index=["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')", 'CodigoPrevisto', 'Franja', 'CodigoSustitucion', 'ABS'],  values = ['Fichero'], aggfunc=len).reset_index().head(20)

# COMMAND ----------

pd.pivot_table(df, index=['CodigoPrevisto', 'Franja'],  values = ['Fichero'], aggfunc=len).reset_index().head(20).append(
pd.pivot_table(df, index=['CodigoSustitucion', 'Franja'],  values = ['Fichero'], aggfunc=len).reset_index().head(20))
