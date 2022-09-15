# Databricks notebook source
#!pip install pandas_profiling
import sys
import pandas_profiling
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

original.tablon_inicial

# COMMAND ----------

original.informe_calidad_dato()

# COMMAND ----------

pd.pivot_table(original.tablon_inicial, index=["('GESTIÓN COBERTURA PUNTO DE VENTA', 'Código')", 'CodigoPrevisto', 'Franja', 'CodigoSustitucion', 'ABS'],  values = ['Fichero'], aggfunc=len).reset_index().head(20)
