"""
###########################################################################
# TRANSFORMA LOS .JSON GENERADOS A PARTIR DE LOS .DAT
###########################################################################
# Este archivo procesa los JSON generados a partir de los ficheros DAT que entrega el INE
# Define nombres de columnas
# Transforma a diferentes formatos: postgresql database, excel file
###########################################################################
"""

##########################################################################
import sys
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import Table
from sqlalchemy.inspection import inspect
#import json
sys.path.append('config')    
from config import const
path_root = const.conf_path_root
path_data_root = const.conf_path_data_root
path_json_root = const.conf_path_json_root
path_static_root = const.conf_path_static_root
path_xlsx_root = const.conf_path_xlsx_root
##########################################################################



############################################
# #hardcoded: para procesar todo
# bool_process = True
# bool_DoSql = True
# bool_DoXls = True

# #hardcoded: no procesa nada
# bool_process = False 
# bool_DoSql = False
# bool_DoXls = False
############################################


def json2other(bool_DoSql, bool_DoXls):

    ############################################
    # para hardcordear el NO procesado de algun archivo en particular
    bool_process_Geo = True #False=no procesa las ubicaciones
    bool_process_3 = True   #False=no procesa dara3.JSON
    bool_process_4 = True   #False=no procesa dara4.JSON
    bool_process_5 = True   #False=no procesa dara5.JSON
    bool_process_6 = True   #False=no procesa dara6.JSON
    bool_process_7 = True   #False=no procesa dara7.JSON
    bool_process_8 = True   #False=no procesa dara8.JSON
    bool_process_9 = True   #False=no procesa dara9.JSON
    bool_process_10 = True  #False=no procesa dara10.JSON
    ############################################

    ###########################################################################
    if bool_DoSql == True:
        # Create the engine to connect to the PostgreSQL database
        pg_engine = sqlalchemy.create_engine(const.conf_postgres_engine_str)
        pg_meta = sqlalchemy.MetaData()        
    ###########################################################################

    ###########################################################################

    if bool_process_Geo==True:
        
        #------------------------------
        # comunidades
        df_comunidad = pd.read_json(path_static_root + '\comunidad.json')
        df_comunidad.columns = ['id_comunidad','nombre_comunidad']
        df_comunidad = df_comunidad.set_index('id_comunidad')
        
        #------------------------------
        # provincias
        df_provincia = pd.read_json(path_static_root + '\provincia.json')
        df_provincia.columns = ['id_comunidad','id_provincia','nombre_provincia']
        df_provincia = df_provincia.set_index(['id_comunidad','id_provincia'])

        #------------------------------
        # municipios
        df_municipio = pd.read_json(path_static_root + '\municipio.json')
        df_municipio.columns = ['id_comunidad','id_provincia','id_municipio','id_distrito','nombre_municipio']
        df_municipio = df_municipio.set_index(['id_comunidad','id_provincia','id_municipio','id_distrito'])

        #arreglo el nombre para los que son con coma: invierto el split por coma
        for indice, fila in df_municipio.iterrows():
            if fila['nombre_municipio'].find(',') > 0:
                fila['nombre_municipio'] = str(fila['nombre_municipio']).split(",")[1].strip() + ' ' + str(fila['nombre_municipio']).split(",")[0].strip()

        #------------------------------
        # union de todos en 1 archivo
        df_ubicaciones = df_comunidad.join(df_provincia.join(df_municipio))

        df_ubicaciones['Comunidad_Completo'] = 'España, ' + df_ubicaciones['nombre_comunidad']
        df_ubicaciones['Provincia_Completo'] = 'España, ' + df_ubicaciones['nombre_comunidad'] + ', ' + df_ubicaciones['nombre_provincia']
        df_ubicaciones['Municipio_Completo'] = 'España, ' + df_ubicaciones['nombre_comunidad'] + ', ' + df_ubicaciones['nombre_provincia'] + ', ' + df_ubicaciones['nombre_municipio']

        # print('ubicaciones:',df_ubicaciones.index)
        # sys.exit("ok")

        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
        if bool_DoSql == True:
            #borro las tablas actuales
            if inspect(pg_engine).has_table('df_comunidad'):
                tabla = Table('df_comunidad', pg_meta)
                tabla.drop(pg_engine)
            if inspect(pg_engine).has_table('df_provincia'):
                tabla = Table('df_provincia', pg_meta)
                tabla.drop(pg_engine)
            if inspect(pg_engine).has_table('df_municipio'):
                tabla = Table('df_municipio', pg_meta)
                tabla.drop(pg_engine)
            if inspect(pg_engine).has_table('df_ubicaciones'):
                tabla = Table('df_ubicaciones', pg_meta)
                tabla.drop(pg_engine)

            #creo la tabla en la base de datos
            df_comunidad.to_sql('df_comunidad',pg_engine)
            df_provincia.to_sql('df_provincia',pg_engine)
            df_municipio.to_sql('df_municipio',pg_engine)
            df_ubicaciones.to_sql('df_ubicaciones',pg_engine)

        if bool_DoXls == True:
            #------------------------------
            # borrado y escritura de archivos excel
            #borra el archivo actual
            tmp_file = path_xlsx_root + 'df_comunidad.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_comunidad.to_excel(tmp_file,merge_cells=False)
            
            #borra el archivo actual
            tmp_file = path_xlsx_root + 'df_provincia.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_provincia.to_excel(tmp_file,merge_cells=False)

            #borra el archivo actual
            tmp_file = path_xlsx_root + 'df_municipio.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_municipio.to_excel(tmp_file,merge_cells=False)

            #borra el archivo actual
            tmp_file = path_xlsx_root + 'df_ubicaciones.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_ubicaciones.to_excel(tmp_file,merge_cells=False)
        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>

    ###########################################################################


    ###########################################################################
    if bool_process_3==True:
        data_name = 'data03'
        data_file = path_json_root + data_name + '.json'

        df_Data = pd.read_json(data_file)

        df_Data.rename(columns = {0: 'TipoEleccion'}, inplace = True) 
        df_Data.rename(columns = {1: 'Ano'}, inplace = True) 
        df_Data.rename(columns = {2: 'Mes'}, inplace = True) 
        df_Data.rename(columns = {3: 'id_candidatura'}, inplace = True) 
        df_Data.rename(columns = {4: 'Siglas', 5: 'Agrupacion'}, inplace = True) 
        df_Data.rename(columns = {6: 'id_candidatura_provincial'}, inplace = True) 
        df_Data.rename(columns = {7: 'id_candidatura_comunidad'}, inplace = True) 
        df_Data.rename(columns = {8: 'id_candidatura_nacional'}, inplace = True) 

        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
        if bool_DoSql == True:
            #borro la tabla
            if inspect(pg_engine).has_table(data_name):
                tabla = Table(data_name, pg_meta)
                tabla.drop(pg_engine)
            
            #creo la tabla en la base de datos
            df_Data.to_sql(data_name,pg_engine)

        if bool_DoXls == True:
            #borra el archivo actual
            tmp_file = path_xlsx_root + data_name + '.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_Data.to_excel(tmp_file,merge_cells=False)
        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>

    ###########################################################################

    ###########################################################################
    if bool_process_4==True:
        data_name = 'data04'
        data_file = path_json_root + data_name + '.json'

        df_Data = pd.read_json(data_file)

        df_Data.rename(columns = {0: 'TipoEleccion'}, inplace = True) 
        df_Data.rename(columns = {1: 'Ano'}, inplace = True) 
        df_Data.rename(columns = {2: 'Mes'}, inplace = True) 
        df_Data.rename(columns = {3: 'Vuelta'}, inplace = True) 
        df_Data.rename(columns = {4: 'id_provincia'}, inplace = True) 
        df_Data.rename(columns = {5: 'id_distrito'}, inplace = True)  
        df_Data.rename(columns = {6: 'id_municipio'}, inplace = True)  
        df_Data.rename(columns = {7: 'id_candidatura'}, inplace = True) 
        df_Data.rename(columns = {8: 'Candidato_Orden'}, inplace = True) 
        df_Data.rename(columns = {9: 'Candidato_Tipo'}, inplace = True) 
        df_Data.rename(columns = {10: 'Candidato_Nombre', 11: 'Candidato_Apellido', 12: 'Candidato_SegundoApellido'}, inplace = True) 
        df_Data.rename(columns = {13: 'Candidato_Sexo'}, inplace = True) 
        df_Data.rename(columns = {14: 'Candidato_Nacimiento_Dia'}, inplace = True) 
        df_Data.rename(columns = {15: 'Candidato_Nacimiento_Mes'}, inplace = True) 
        df_Data.rename(columns = {16: 'Candidato_Nacimiento_Ano'}, inplace = True) 
        df_Data.rename(columns = {17: 'Candidato_DNI'}, inplace = True) 
        df_Data.rename(columns = {18: 'Elegido'}, inplace = True) 

        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
        if bool_DoSql == True:
            #borro la tabla
            if inspect(pg_engine).has_table(data_name):
                tabla = Table(data_name, pg_meta)
                tabla.drop(pg_engine)
            #creo la tabla en la base de datos
            df_Data.to_sql(data_name,pg_engine)

        if bool_DoXls == True:
            #borra el archivo actual
            tmp_file = path_xlsx_root + data_name + '.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_Data.to_excel(tmp_file,merge_cells=False)
        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>

    ###########################################################################

    ###########################################################################
    if bool_process_5==True:
        data_name = 'data05'
        data_file = path_json_root + data_name + '.json'

        df_Data = pd.read_json(data_file)

        df_Data.rename(columns = {0: 'TipoEleccion'}, inplace = True) 
        df_Data.rename(columns = {1: 'Ano'}, inplace = True) 
        df_Data.rename(columns = {2: 'Mes'}, inplace = True) 
        df_Data.rename(columns = {3: 'Vuelta'}, inplace = True) 
        df_Data.rename(columns = {4: 'id_comunidad', 5: 'id_provincia', 6: 'id_municipio'}, inplace = True)  
        df_Data.rename(columns = {7: 'id_distrito'}, inplace = True) 
        df_Data.rename(columns = {8: 'nombre_municipio'}, inplace = True) 
        df_Data.rename(columns = {9: 'CodigoElectoral'}, inplace = True) 
        df_Data.rename(columns = {10: 'CodigoPartidoJudicial'}, inplace = True) 
        df_Data.rename(columns = {11: 'CodigoDiputacionJudicial'}, inplace = True) 
        df_Data.rename(columns = {12: 'CodigoComarca'}, inplace = True) 
        df_Data.rename(columns = {13: 'PoblacionDerecho'}, inplace = True) 
        df_Data.rename(columns = {14: 'NumeroMesas'}, inplace = True) 
        df_Data.rename(columns = {15: 'CensoINE'}, inplace = True) 
        df_Data.rename(columns = {16: 'CensoEscrutinio'}, inplace = True) 
        df_Data.rename(columns = {17: 'CensoCERE'}, inplace = True) 
        df_Data.rename(columns = {18: 'VotosCERE'}, inplace = True) 
        df_Data.rename(columns = {19: 'Votos1Avance'}, inplace = True) 
        df_Data.rename(columns = {20: 'Votos2Avance'}, inplace = True) 
        df_Data.rename(columns = {21: 'VotosBlancos'}, inplace = True) 
        df_Data.rename(columns = {22: 'VotosNulos'}, inplace = True) 
        df_Data.rename(columns = {23: 'VotosCandidaturas'}, inplace = True) 
        df_Data.rename(columns = {24: 'NumeroEscanos'}, inplace = True) 
        df_Data.rename(columns = {25: 'VotosReferendumSi'}, inplace = True) 
        df_Data.rename(columns = {26: 'VotosReferendumNo'}, inplace = True) 
        df_Data.rename(columns = {27: 'DatosOficiales'}, inplace = True) 

        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
        if bool_DoSql == True:
            #borro la tabla
            if inspect(pg_engine).has_table(data_name):
                tabla = Table(data_name, pg_meta)
                tabla.drop(pg_engine)
            #creo la tabla en la base de datos
            df_Data.to_sql(data_name,pg_engine)

        if bool_DoXls == True:
            #borra el archivo actual
            tmp_file = path_xlsx_root + data_name + '.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_Data.to_excel(tmp_file,merge_cells=False)
        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
    ###########################################################################

    ###########################################################################
    if bool_process_6==True:
        data_name = 'data06'
        data_file = path_json_root + data_name + '.json'

        df_Data = pd.read_json(data_file)

        df_Data.rename(columns = {0: 'TipoEleccion'}, inplace = True) 
        df_Data.rename(columns = {1: 'Ano'}, inplace = True) 
        df_Data.rename(columns = {2: 'Mes'}, inplace = True) 
        df_Data.rename(columns = {3: 'Vuelta'}, inplace = True) 
        df_Data.rename(columns = {4: 'id_provincia', 5: 'id_municipio'}, inplace = True)  
        df_Data.rename(columns = {6: 'id_distrito'}, inplace = True) 
        df_Data.rename(columns = {7: 'id_candidatura'}, inplace = True) 
        df_Data.rename(columns = {8: 'Votos'}, inplace = True) 
        df_Data.rename(columns = {9: 'candidatos_electos'}, inplace = True) 

        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
        if bool_DoSql == True:
            #borro la tabla
            if inspect(pg_engine).has_table(data_name):
                tabla = Table(data_name, pg_meta)
                tabla.drop(pg_engine)
            #creo la tabla en la base de datos
            df_Data.to_sql(data_name,pg_engine)

        if bool_DoXls == True:
            #borra el archivo actual
            tmp_file = path_xlsx_root + data_name + '.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_Data.to_excel(tmp_file,merge_cells=False)
        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
    ###########################################################################

    ###########################################################################
    if bool_process_7==True:
        data_name = 'data07'
        data_file = path_json_root + data_name + '.json'

        df_Data = pd.read_json(data_file)

        df_Data.rename(columns = {0: 'TipoEleccion'}, inplace = True) 
        df_Data.rename(columns = {1: 'Ano'}, inplace = True) 
        df_Data.rename(columns = {2: 'Mes'}, inplace = True) 
        df_Data.rename(columns = {3: 'Vuelta'}, inplace = True) 
        df_Data.rename(columns = {4: 'id_comunidad', 5: 'id_provincia'}, inplace = True) 
        df_Data.rename(columns = {6: 'id_distrito'}, inplace = True) 
        df_Data.rename(columns = {7: 'NombreAmbitoTerritorial'}, inplace = True) 
        df_Data.rename(columns = {8: 'PoblacionDerecho'}, inplace = True) 
        df_Data.rename(columns = {9: 'NumeroMesas'}, inplace = True) 
        df_Data.rename(columns = {10: 'CensoINE'}, inplace = True) 
        df_Data.rename(columns = {11: 'CensoEscrutinio'}, inplace = True) 
        df_Data.rename(columns = {12: 'CensoCERE'}, inplace = True) 
        df_Data.rename(columns = {13: 'VotosCERE'}, inplace = True) 
        df_Data.rename(columns = {14: 'Votos1Avance'}, inplace = True) 
        df_Data.rename(columns = {15: 'Votos2Avance'}, inplace = True) 
        df_Data.rename(columns = {16: 'VotosBlancos'}, inplace = True) 
        df_Data.rename(columns = {17: 'VotosNulos'}, inplace = True) 
        df_Data.rename(columns = {18: 'VotosCandidaturas'}, inplace = True) 
        df_Data.rename(columns = {19: 'NumeroEscanos'}, inplace = True) 
        df_Data.rename(columns = {20: 'VotosReferendumSi'}, inplace = True) 
        df_Data.rename(columns = {21: 'VotosReferendumNo'}, inplace = True) 
        df_Data.rename(columns = {22: 'DatosOficiales'}, inplace = True) 

        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
        if bool_DoSql == True:
            #borro la tabla
            if inspect(pg_engine).has_table(data_name):
                tabla = Table(data_name, pg_meta)
                tabla.drop(pg_engine)
            #creo la tabla en la base de datos
            df_Data.to_sql(data_name,pg_engine)

        if bool_DoXls == True:
            #borra el archivo actual
            tmp_file = path_xlsx_root + data_name + '.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_Data.to_excel(tmp_file,merge_cells=False)
        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
    ###########################################################################

    ###########################################################################
    if bool_process_8==True:
        data_name = 'data08'
        data_file = path_json_root + data_name + '.json'

        df_Data = pd.read_json(data_file)

        df_Data.rename(columns = {0: 'TipoEleccion'}, inplace = True) 
        df_Data.rename(columns = {1: 'Ano'}, inplace = True) 
        df_Data.rename(columns = {2: 'Mes'}, inplace = True) 
        df_Data.rename(columns = {3: 'Vuelta'}, inplace = True) 
        df_Data.rename(columns = {4: 'id_comunidad', 5: 'id_provincia', 6: 'id_municipio'}, inplace = True)  
        df_Data.rename(columns = {7: 'id_candidatura'}, inplace = True) 
        df_Data.rename(columns = {8: 'votos'}, inplace = True) 
        df_Data.rename(columns = {9: 'candidatos_electos'}, inplace = True) 

        df_Data = df_Data[(df_Data['id_comunidad'] != 99)]
        df_Data = df_Data[(df_Data['id_provincia'] != 99)]
        df_Data = df_Data[(df_Data['id_municipio'] != 999)]

        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
        if bool_DoSql == True:
            #borro la tabla
            if inspect(pg_engine).has_table(data_name):
                tabla = Table(data_name, pg_meta)
                tabla.drop(pg_engine)
            #creo la tabla en la base de datos
            df_Data.to_sql(data_name,pg_engine)

        if bool_DoXls == True:
            #borra el archivo actual
            tmp_file = path_xlsx_root + data_name + '.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_Data.to_excel(tmp_file,merge_cells=False)
        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
    ###########################################################################

    ###########################################################################
    if bool_process_9==True:
        data_name = 'data09'
        data_file = path_json_root + data_name + '.json'

        df_Data = pd.read_json(data_file)

        df_Data.rename(columns = {0: 'TipoEleccion'}, inplace = True) 
        df_Data.rename(columns = {1: 'Ano'}, inplace = True) 
        df_Data.rename(columns = {2: 'Mes'}, inplace = True) 
        df_Data.rename(columns = {3: 'Vuelta'}, inplace = True) 
        df_Data.rename(columns = {4: 'id_comunidad', 5: 'id_provincia', 6: 'id_municipio'}, inplace = True)  
        df_Data.rename(columns = {7: 'id_distrito'}, inplace = True) 
        df_Data.rename(columns = {8: 'CodigoSeccion'}, inplace = True) 
        df_Data.rename(columns = {9: 'CodigoMesa'}, inplace = True) 
        df_Data.rename(columns = {10: 'Censo'}, inplace = True) 
        df_Data.rename(columns = {11: 'Votos'}, inplace = True) 
        df_Data.rename(columns = {12: 'CensoCERE'}, inplace = True) 
        df_Data.rename(columns = {13: 'VotosCERE'}, inplace = True) 
        df_Data.rename(columns = {14: 'Votos1Avance'}, inplace = True) 
        df_Data.rename(columns = {15: 'Votos2Avance'}, inplace = True) 
        df_Data.rename(columns = {16: 'VotosBlancos'}, inplace = True) 
        df_Data.rename(columns = {17: 'VotosNulos'}, inplace = True) 
        df_Data.rename(columns = {18: 'VotosCandidaturas'}, inplace = True) 
        df_Data.rename(columns = {19: 'VotosReferendumSi'}, inplace = True) 
        df_Data.rename(columns = {20: 'VotosReferendumNo'}, inplace = True) 
        df_Data.rename(columns = {21: 'DatosOficiales'}, inplace = True) 

        #borro los registros de voto CERA (españoles viviendo en el extranjero, por no tener ubicacion, se podria ajustar)
        # df_Data = df_Data[(df_Data[4] != 99)]
        # df_Data = df_Data[(df_Data[5] != 99)]
        # df_Data = df_Data[(df_Data[6] != 999)]    #119.593

        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
        if bool_DoSql == True:
            #borro la tabla
            if inspect(pg_engine).has_table(data_name):
                tabla = Table(data_name, pg_meta)
                tabla.drop(pg_engine)
            #creo la tabla en la base de datos
            df_Data.to_sql(data_name,pg_engine)

        if bool_DoXls == True:
            #borra el archivo actual
            tmp_file = path_xlsx_root + data_name + '.xlsx'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            df_Data.to_excel(tmp_file,merge_cells=False)
        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
    ###########################################################################

    ###########################################################################
    if bool_process_10==True:
        data_name = 'data10'
        data_file = path_json_root + data_name + '.json'

        df_Data = pd.read_json(data_file)

        df_Data.rename(columns = {0: 'TipoEleccion'}, inplace = True) 
        df_Data.rename(columns = {1: 'Ano'}, inplace = True) 
        df_Data.rename(columns = {2: 'Mes'}, inplace = True) 
        df_Data.rename(columns = {3: 'Vuelta'}, inplace = True) 
        df_Data.rename(columns = {4: 'id_comunidad', 5: 'id_provincia', 6: 'id_municipio'}, inplace = True)  
        df_Data.rename(columns = {7: 'id_distrito'}, inplace = True) 
        df_Data.rename(columns = {8: 'CodigoSeccion'}, inplace = True) 
        df_Data.rename(columns = {9: 'CodigoMesa'}, inplace = True) 
        df_Data.rename(columns = {10: 'id_candidatura'}, inplace = True) 
        df_Data.rename(columns = {11: 'Votos'}, inplace = True) 

        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
        if bool_DoSql == True:
            #borro la tabla
            if inspect(pg_engine).has_table(data_name):
                tabla = Table(data_name, pg_meta)
                tabla.drop(pg_engine)
            #creo la tabla en la base de datos
            df_Data.to_sql(data_name,pg_engine)

        if bool_DoXls == True:
            #borra el archivo actual
            tmp_file = path_xlsx_root + data_name + '.csv'
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
            #crea el nuevo archivo
            #       excel no porque es muy grande el archivo para los limites del excel:
            #       df_Data.to_excel(tmp_file,merge_cells=False)
            df_Data.to_csv(tmp_file, index=False)
        #>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>#>>>>>>>>>>>>>>
    ###########################################################################

    return 'OK'
