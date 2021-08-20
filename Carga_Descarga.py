###########################################################
#
#       IMPORTS
#
###########################################################

import pandas as pd
import numpy as np
from gspread_pandas import Spread, conf
import datetime
import calendar

###########################################################
#
#       CREDENCIALES
#
###########################################################

# Credenciales
cred = conf.get_config('..\\', 'PedidosYa-8b8c4d19f61c.json')

###########################################################
#
#       FUNCION RR
#
###########################################################

def func_rr(viernes=1.4,sabado=1.4,domingo=1.4):
    '''     
        Esta función recibe 3 parámetros optativos y devuelve el RR del mes:
        . Viernes, Sábado y Domingo que representan la ponderación de dichos días
        En caso de ser primero del mes siguiente, la misma devuelve 1.
        En caso de errores:
        . Devuelve vacío y hace un print de un mensaje de error de cálculo
    '''
    try:
        # Fechas
        hoy = datetime.date.today()
        if hoy.day == 1:
            rr = 1
        else:
            hoy = hoy
            # Armo el RR
            calendario = calendar.monthcalendar(hoy.year,hoy.month)
            dias = {0:0,1:0,2:0,3:0,4:0,5:0,6:0}
            transcurridos = {0:0,1:0,2:0,3:0,4:0,5:0,6:0}
            mes = [dias,transcurridos]
            hoy = hoy.day
            for i in range(7):
                for j in calendario:
                    if j[i] != 0:
                        dias[i] += 1
                    if j[i] < hoy and j[i] != 0:
                        transcurridos[i] +=1
            final_rr = [0,0]
            cont = 0
            for j in mes:
                for i in j:
                    if i < 4:
                        final_rr[cont] += j[i]
                    elif i == 4:
                        final_rr[cont] += j[i] * viernes
                    elif i == 5:
                        final_rr[cont] += j[i] * sabado
                    else:
                        final_rr[cont] += j[i] * domingo
                cont += 1
            rr = 0
            if final_rr[1] == 0:
                rr = 0.01
            else:
                rr = final_rr[1] / final_rr[0]
        return rr
    except:
        print('Error en el Cálculo')
        return

###########################################################
#
#       FUNCION DESCARGA
#
###########################################################

def descarga(sheet_id,wks_name):
    '''     
        Esta función recibe dos parámetros:
        . Id de la Sheet de donde se descargará la información
        . Nombre de la pestaña de la cual se descargará la información
        Devuelve el DF descargado
        En caso de errores:
        . Devuelve vacío y hace un print de un mensaje de error de descarga
    '''
    try:
        sheet = Spread(sheet_id, wks_name, config=cred)
        df = sheet.sheet_to_df(index=0,header_rows=1)
        print('Descarga Correcta!')
        return df
    except:
        print('Error en Descarga!')
        return

###########################################################
#
#       FUNCION CARGA
#
###########################################################

today = str(datetime.date.today())
log = pd.DataFrame(columns=['Archivo',str(datetime.date.today())])

def carga(df,sheet_id,wks_name,archivo=None,log=None):
    '''     
        Esta función recibe cinco parámetros:
        . DF a cargar
        . Id de la Sheet donde se cargará el DF
        . Nombre de la pestaña donde se cargará el DF
        . Nombre del archivo para su logeo (solo necesario si quiere logearse los resultados)
        . El DF de Logeo (solo necesario si quiere logearse los resultados)
        En caso de querer hacer un logeo devuelve el DF de Logeo actualizado
    '''
    try:
        sheet = Spread(sheet_id, wks_name, config=cred)
        sheet.df_to_sheet(df, index=False, sheet=wks_name, replace=True)
        if log is None:
            print('Carga Correcta!')
        else:
            log = log.append({'Archivo': archivo, today: 'Correcto'}, ignore_index=True)
            return log
    except:
        if log is None:
            print('Error en Carga!')
        else:
            log = log.append({'Archivo': archivo, today: 'Error'}, ignore_index=True)
            return log