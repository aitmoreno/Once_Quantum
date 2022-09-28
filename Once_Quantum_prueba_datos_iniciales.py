# Databricks notebook source
!pip install pandas_profiling
import sys
import pandas_profiling
from pandas_profiling import ProfileReport
sys.path.append('/dbfs/FileStore/tables/ONCE_Quantum')
from Once_Quantum_ETL_library import Once_Quantum_ETL
import pandas as pd 



# COMMAND ----------

###########################################################################################################
#Cargamos los datos almacenados en el proceso de Ingesta, si no queremos reprocesar de nuevo los ficheros
###########################################################################################################
#Inicio de proceso post-ingesta
original = Once_Quantum_ETL('/dbfs/FileStore/tables/ONCE_Quantum/')
original.leer_datos_iniciales()

# COMMAND ----------

display(original.tablon_inicial)

# COMMAND ----------

print(original.tablon_pv_vendedor['Código punto de venta'].astype('str').describe())
print(original.tablon_inicial["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')"].astype('str').describe()) #Todos
print(original.tablon_inicial[original.tablon_inicial['PV_Asignado'] == 1]["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')"].astype('str').describe()) #Con PV Asigando

###############################################################################################
# Seleccionamos sólo los Asignados
original_necesidad_pv = original.tablon_inicial[original.tablon_inicial['PV_Asignado'] == 1] 
###############################################################################################
      
      #["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')"].astype('str').describe()) #Con PV Asignado

# COMMAND ----------

print(original.tablon_pv_vendedor['Código punto de venta'].astype('str').describe())
print(original.tablon_inicial["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')"].astype('str').describe())
print(original_necesidad_pv["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')"].astype('str').describe())

# COMMAND ----------

#Puntos de Venta en los diarios, que no están activos
lista_pv_no_validos = original.tablon_inicial[original.tablon_inicial['PV_Asignado'] != 1].iloc[:,1].unique()
lista_pv_no_validos.shape
display(pd.DataFrame(lista_pv_no_validos))

# COMMAND ----------

# DBTITLE 1,Se demuestra que no hay puntos de venta del diario que no estén en la asiganción a vendedores y que tengan Abstención
print(original.tablon_inicial[original.tablon_inicial['PV_Asignado'] == 0].ABS.describe())
original.tablon_inicial[(original.tablon_inicial['PV_Asignado'] == 0) & (original.tablon_inicial['ABS'].notnull())]

# COMMAND ----------

print(original.tablon_inicial[original.tablon_inicial['PV_Asignado'].isnull()]['PV_Asignado'].describe())
original.tablon_inicial[(original.tablon_inicial['PV_Asignado'].isnull()) & (original.tablon_inicial['PV_Asignado'].notnull())]

# COMMAND ----------

#Puntos de ventas en las asiganciones de vendedores que no están en los diarios
lista_diarios = original.tablon_inicial["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')"].unique()
listas_diarios_no_asignacion = original.tablon_pv_vendedor[~original.tablon_pv_vendedor['Código punto de venta'].isin (lista_diarios)].iloc[:,3].unique()
listas_diarios_no_asignacion.shape
display(pd.DataFrame(listas_diarios_no_asignacion))


# COMMAND ----------

# No funciona la visualización HTML como módulo en Notebooks
# original.informe_calidad_dato()

prof = ProfileReport(original_necesidad_pv)
displayHTML(prof.to_html()) 

# COMMAND ----------

#Con la asignación de Puntos de ventas a Vendedores
original.tablon_inicial.to_csv(original.input_path + 'Tablon_Inicial_Necesidad.csv') 

#Acordarse de generar una tabla DAtaBase desde el origen DBFS con este CSV para cargarlo en PowerBI

# COMMAND ----------

# DBTITLE 1,Las siguientes celdas son ejemplos de queries
####################################################################################

# COMMAND ----------

pd.pivot_table(original.tablon_inicial, index=["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')", 'CodigoPrevisto', 'Franja', 'CodigoSustitucion', 'ABS'],  values = ['Fichero'], aggfunc=len).reset_index().head(20)

# COMMAND ----------

#Hipótesis de nulos por prioridad
#Chequeamos aquellos puestos que si que tienen Necesidad
df = original.tablon_inicial[original.tablon_inicial['Necesidad'] == 1]

print(df[df['CodigoPrevisto'].isnull()].count())

#pd.options.display.max_rows = 20
pivotado = pd.pivot_table(df[df['CodigoPrevisto'].isnull()], index=['SegPV'],  values = ['CodigoPrevisto'], aggfunc=len).merge(df.groupby(['SegPV'])["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')"].count().to_frame(), left_on='SegPV', right_on='SegPV').rename(columns={"CodigoPrevisto": "Codigos NO Asigandos", "('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')": "Total"})
pivotado['porcentaje_NO_asignados'] = pivotado.iloc[:,0] / pivotado.iloc[:,1]
pivotado['Asignados'] =  pivotado.iloc[:,1] - pivotado.iloc[:,0]
print(pivotado.sum())
print(original.tablon_inicial['SegPV'].value_counts().sum())
pivotado

# COMMAND ----------

#Sin absentismo y asignados
print(pd.pivot_table(df, index=['SegPV', 'CodigoPrevisto'],  values = ['Franja'], aggfunc=len).sum())
      
pd.pivot_table(df, index=['SegPV', 'CodigoPrevisto'],  values = ['Franja'], aggfunc=len)

# COMMAND ----------

pd.pivot_table(df, index=[ 'CodigoPrevisto', 'CodigoSustitucion', 'ABS'],  values = ['Fichero'], aggfunc=len).reset_index().head(20)

# COMMAND ----------

pd.pivot_table(df, index=["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')", 'CodigoPrevisto', 'Franja', 'CodigoSustitucion', 'ABS'],  values = ['Fichero'] as 'count', aggfunc=len).reset_index().head(20)

# COMMAND ----------

#Calidad de Datos sólo de los centros/franjas con Necesidad de Ocupación
#Chequeamos aquellos puestos que si que tienen Necesidad
df = original.tablon_inicial[original.tablon_inicial['Necesidad'] == 1]

profNecesarias = ProfileReport(df)
displayHTML(profNecesarias.to_html()) 

# COMMAND ----------

profNecesarias

# COMMAND ----------

df = original.tablon_inicial[original.tablon_inicial['Necesidad'] == 1]


pivotado = pd.pivot_table(df[df['CodigoPrevisto'].isnull()], index=['SegPV'],  values = ['CodigoPrevisto'], aggfunc=len).merge(df.groupby(['SegPV'])["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')"].count().to_frame(), left_on='SegPV', right_on='SegPV').rename(columns={"CodigoPrevisto": "Codigos NO Asigandos", "('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')": "Total"})
pivotado['porcentaje_NO_asignados'] = pivotado.iloc[:,0] / pivotado.iloc[:,1]
pivotado['Asignados'] =  pivotado.iloc[:,1] - pivotado.iloc[:,0]
print(pivotado.sum())
print(original.tablon_inicial['SegPV'].value_counts().sum())
pivotado

# COMMAND ----------

df[df['CodigoPrevisto'].isnull()]

