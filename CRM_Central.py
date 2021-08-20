###########################################################
#
#       IMPORTS
#
###########################################################

import pandas as pd
import numpy as np
import pandas_gbq
import datetime
from dateutil.relativedelta import relativedelta
from gspread_pandas import Spread, conf
from ast import literal_eval
from Carga_Descarga import *

###########################################################
#
#       CONEXIONES GOOGLE SHEETS
#
###########################################################

# Mapeo Campañas
sheet_id = '1d4cMi-V04c80Dpen4S70Q7zYqqho3ZBIVctlRIBLwiw'
wks_name = 'Mapeo Campañas'
sheet = Spread(sheet_id, wks_name, config=cred)
mapeo = sheet.sheet_to_df(index=0,header_rows=1)

# IVA
sheet_id = '1d4cMi-V04c80Dpen4S70Q7zYqqho3ZBIVctlRIBLwiw'
wks_name = 'IVA'
sheet = Spread(sheet_id, wks_name, config=cred)
iva = sheet.sheet_to_df(index=0,header_rows=1)

# Listas
sheet_id = '1d4cMi-V04c80Dpen4S70Q7zYqqho3ZBIVctlRIBLwiw'
wks_name = 'Listas'
sheet = Spread(sheet_id, wks_name, config=cred)
listas = sheet.sheet_to_df(index=0,header_rows=1)

# Filtro Campañas
sheet_id = '1d4cMi-V04c80Dpen4S70Q7zYqqho3ZBIVctlRIBLwiw'
wks_name = 'Filtrar'
sheet = Spread(sheet_id, wks_name, config=cred)
fc = sheet.sheet_to_df(index=0,header_rows=1)

###########################################################
#
#       FUNCION MAPEO CAMPAÑAS
#
###########################################################

# Dropeo las columnas vacias
mapeo.drop([''],axis=1,inplace=True)
# Divido el DF en varios
cols = len(mapeo.columns)
dfs = []
for i in range(int(cols/3)):
    dfs.append(mapeo.iloc[:,i*3:i*3+3].copy())
# Borro las filas vacias de cada DF
dfs = [x[x['Columna'] != ''] for x in dfs]
dfs = pd.concat(dfs,ignore_index=True).copy()
# Creo un diccionario de variables
records = {}
filters = dfs['Columna'].unique().tolist()
for i in filters:
    records[i] = {}
# Completo el diccionario de variables
for i in records:
    records[i] = dfs[dfs['Columna'] == i].set_index(['Search']).to_dict()['Value']

# Defino la funcion de Filtros
def filtros(cam,filtro,benefit=None):
    '''     
        Esta función recibe tres parámetros:
        . Nombre de la Campaña
        . Nombre del Filtro
        . El benefit en caso de querer obligar a WALLET a contar como WALLET en el Type (opcional)
        En caso de errores:
        . Si no existe el filtro devuelve "No Existe Filtro"
        . Si falla luego devuelve "Error en Comparacion"
    '''
    try:
        dicc = records[filtro]
    except:
        return 'No Existe Filtro'
    flag = 0
    try:
        for i in dicc:
            if i in str(cam):
                val = dicc[i]
                flag = 1
                break
        if flag == 0:
            val = dicc[list(dicc.keys())[-1]]
        if benefit is None:
            return val
        elif benefit == 'WALLET':
            return 'WALLET'
        else:
            return val
    except:
        return 'Error en Comparacion'

###########################################################
#
#       FUNCION IVA
#
###########################################################

# Reemplazo los vacíos por 1
iva.replace([''],0,inplace=True)
# Paso los valores a mayúsculas
iva['Country'] = iva['Country'].str.upper()
# Cambio los países con acentos
acentos = literal_eval(listas[listas['Tipo'] == 'Paises']['Lista'].values[0])
def cambiar_paises(cou):
    flag = 0
    try:
        for i in acentos:
            if cou == i:
                val = acentos[i]
                flag = 1
        if flag == 0:
            val = cou
        return val
    except:
        return cou
iva['Country'] = iva['Country'].apply(cambiar_paises)
# Doy formato a las columnas
iva[['IVA','Extra']] = iva[['IVA','Extra']].astype(float)

# Defino la funcion de IVA
def func_iva(df):
    '''     
        Esta función recibe un parámetro:
        . El DF
        En caso de errores:
        . Devuelve el DF como estaba
        . Devuelve el error (falta columna u otro)
    '''
    try:
        df['Country'] = df['Country'].str.upper()
        df = df.merge(iva,on=['Country'],how='left').copy()
        df['IVA'].replace([np.nan,np.inf,-np.inf],0,inplace=True)
        df['Extra'].replace([np.nan,np.inf,-np.inf],1,inplace=True)
        return df
    except:
        if 'Country' in list(df.columns):
            print('Surgio otro Error')
            return df.copy()
        else:
            print('No existe la Columna Country')
            return df.copy()

###########################################################
#
#       FUNCION FILTRO CAMPAÑAS
#
###########################################################

# Mando a mayusculas los filtros
fc[fc.columns[0]] = fc[fc.columns[0]].str.upper()
fc = fc[fc.columns[0]].tolist()

# Defino la funcion de filtro
def filtrar_cam(cam):
    '''     
        Esta función recibe un parámetro:
        . Nombre de la campaña
        En caso de errores:
        . Devuelve el mensaje "Error en Comparacion" y el valor default
        . Por default devuelve "No"
    '''
    val = 'No'
    try:
        for j in fc:
            if j in cam:
                val = 'Si'
                break
        return val
    except:
        print('Error en Comparacion')
        return val