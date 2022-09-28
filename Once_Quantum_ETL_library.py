# Databricks notebook source
#!pip install xlrd
#!pip install openpyxl
#!pip install pandas_profiling

# COMMAND ----------

#ETL inicial de los datos
import pandas as pd
import glob
import numpy as np
import pandas_profiling
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
    
  def leer_datos_vendedor_puntodeventa(self, fichero):
    ficheroresult = pd.DataFrame() 
    vendedor_pv = pd.read_excel(fichero, engine='openpyxl', sheet_name = 'Inf Asig Vend-Pv', header = [5])
    return vendedor_pv
  
  def leer_datos_vendedor(self, fichero):
    ficheroresult = pd.DataFrame() 
    vendedor = pd.read_excel(fichero, engine='openpyxl', sheet_name = 'GESCOM', header = [0])
    return vendedor

  
  def procesar_directorio(self):
    path = self.input_path
    self.tablon_inicial = pd.DataFrame() 
    self.tablon_pv_vendedor = pd.DataFrame()
    self.tablon_vendedores = pd.DataFrame()
    filenames = glob.glob(path + "*.xlsx")
    for file in filenames:
     print("Reading file = ",file)
     self.tablon_inicial =  self.tablon_inicial.append(self.procesar_fichero(file) , ignore_index=True) #Acumulamos la información de los ficheros individuales
     self.tablon_inicial = self.tablon_inicial.replace(r'^\s*$', np.nan, regex=True) #Sustituimos espacios en blanco y '' en na
     self.tablon_inicial = self.tablon_inicial.replace(r'nan', np.nan, regex=True) #Sustituimos nan de excel en na de numpy
     #Leemos también las relaciones entre puntos de venta y vendedores
     self.tablon_pv_vendedor = self.tablon_pv_vendedor.append(self.leer_datos_vendedor_puntodeventa(file), ignore_index=True)
      #Leemos también los vendedores
     self.tablon_vendedores = self.tablon_vendedores.append(self.leer_datos_vendedor(file), ignore_index=True)
    #Grabamos el resultado final en formato DBFS
    self.tablon_inicial.to_csv(path + 'Tablon_Inicial.csv')
    self.tablon_pv_vendedor.to_csv(path + 'Tablon_PV_Vendedor.csv')
    self.tablon_vendedores.to_csv(path + 'Tablon_Vendedores.csv')
    
  def leer_datos_iniciales(self):
    #Cargamos los datos almacenados en el proceso de Ingesta
    self.tablon_inicial = pd.DataFrame() 
    self.tablon_inicial = pd.read_csv('/dbfs/FileStore/tables/ONCE_Quantum/Tablon_Inicial.csv', parse_dates=True)
    #Aseguramos que los Codigos de vendedores sean categóricos
    self.tablon_inicial = self.tablon_inicial.astype({'CodigoPrevisto': str, 'CodigoSustitucion': str, 'ABS': str})
    self.tablon_inicial['CodigoPrevisto'] = self.tablon_inicial['CodigoPrevisto'].replace(r'd*\.0', '', regex=True).astype(str)
    self.tablon_inicial['CodigoSustitucion'] = self.tablon_inicial['CodigoSustitucion'].replace(r'd*\.0', '', regex=True).astype(str)
    self.tablon_inicial['CodigoPrevisto'] = self.tablon_inicial['CodigoPrevisto'].replace('nan', np.nan, regex=False)
    self.tablon_inicial['CodigoSustitucion'] = self.tablon_inicial['CodigoSustitucion'].replace('nan', np.nan, regex=False)
    self.tablon_inicial['ABS'] = self.tablon_inicial['ABS'].replace('nan', np.nan, regex=False)
    
    #Incluimos el día de la fecha para compararlo con los cuadrantes necesarios
    days = {0:'L', 1:'M', 2:'X', 3:'J', 4:'V', 5:'S', 6:'D'}
    self.tablon_inicial ['DiaSemana'] = pd.to_datetime(self.tablon_inicial['Fecha']).dt.dayofweek.apply(lambda x: days[x])
    #Calculamos la necesidad de asignar Vendedor
    ListaNecesidad = []
    for index, row in self.tablon_inicial.iterrows():
      entrada = row['DiaSemana']
      grupo = row["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Días operativos')"]
      Necesario = 0
      if type(grupo) == str: #No es nan
        if entrada in (list(grupo)):
          Necesario = 1
      ListaNecesidad = np.append(ListaNecesidad, Necesario)
    #Genearmos la columna de necesidad
    self.tablon_inicial['Necesidad'] = ListaNecesidad
    #Leemos el fichero de puntos de venta por vendedor
    self.tablon_pv_vendedor = pd.read_csv('/dbfs/FileStore/tables/ONCE_Quantum/Tablon_PV_Vendedor.csv', parse_dates=True)
    #Generamos la outer join entre el diario y los PV asigandos a Vendedor
    lista_pv = self.tablon_pv_vendedor['Código punto de venta'].unique()
    self.tablon_inicial['PV_Asignado'] = self.tablon_inicial["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')"].apply(lambda x: 1 if x in (lista_pv) else 0)
    #Leemos los vendedores
    self.tablon_vendedores = pd.read_csv('/dbfs/FileStore/tables/ONCE_Quantum/Tablon_PV_Vendedor.csv', parse_dates=True)
    
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
    #self.prof = ProfileReport(self.tablon_inicial.iloc[:,12:21])
    self.prof = ProfileReport(self.tablon_inicial)
    #self.prof.to_file(output_file='/dbfs/FileStore/tables/ONCE_Quantum/informe.html')
    displayHTML(self.prof.to_html()) 
  
