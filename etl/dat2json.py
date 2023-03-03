
"""
###########################################################################
# PROCESA LOS ARCHIVOS DAT QUE ENTREGA EL INE
###########################################################################
# Loop por todos los archivos .DAT del directorio root de datos.
# Se definen columnas segun el documento "FICHEROS.doc" que entrega el INE.
# Se crean los archivos .JSON con el mismo nombre, pero en otro directorio, 
# con todo el contenido de cada tipo de fichero en 1 solo archivo.
# EJ: si se suben multiples carpetas con data05.DAT, se creara solo 1 archivo data05.JSON
# con todo el contenido junto.
# Mientras mas contenido se procese, mas va a tardar y mas grande sera el archivo .JSON final.
###########################################################################
"""

#################################
import pandas as pd
import json, os, sys
from glob import iglob
sys.path.append('config')    
from config import const
from config import const_json
#################################


def dat2json():

    #################################
    # directorios donde va a trabajar
    path_data_root = const.conf_path_data_root
    path_json_root = const.conf_path_json_root
    #################################


    #################################
    #todos los cortes
    pd_cortes = pd.DataFrame(const_json.const_list_cortes)
    #################################



    #################################
    # 0. loop por tipos de ficheros (analizo todos los 01 juntos, 02 juntos, etc)
    for tf in const_json.const_list_tipoficheros:
        
        tipo_fichero = ('0' + str(tf[0]))[-2:]
        
        #================================
        #0.0 directorio y selector para el loop de esta vuelta
        rootdir_glob = path_data_root + '/**/' # Note the added asterisks
        rootdir_glob += tipo_fichero
        rootdir_glob += '*.DAT'#SOLO FICHEROS .DAT

        #0.1 lista de archivos a procesar en esta vuelta
        files = [f for f in iglob(rootdir_glob, recursive=True) if os.path.isfile(f)]
        file_list = []
        for f in files:    
            file_now = [str(f).split("\\",2)[1] , str(f).split("\\",2)[2]]
            file_list.append(file_now)

        #0.2 cortes de este tipo de fichero
        df_file_cortes = pd.DataFrame(pd_cortes[pd_cortes[0]==tf[0]])
        df_file_cortes_count = df_file_cortes[1]
        df_file_cortes = {'Start':df_file_cortes[2], 'End':df_file_cortes[3]}
        df_file_cortes = pd.DataFrame(df_file_cortes)
        
        #0.3 df destino de todos los df del mismo tipo
        df_list = pd.DataFrame()
        
        #================================
        # 1. loop por archivos
        for f in file_list:

            #================================
            # 2. lo paso a panda dataframe        
            try:

                df_file_data = pd.read_fwf(open(path_data_root + f[0] + '/'+ f[1], 'r', 
                                            encoding='latin-1'),
                                            colspecs=df_file_cortes.values.tolist(),
                                            names=df_file_cortes_count.values.tolist(),
                                            index_col=False)
                
                #================================
                # 3. junto todos los dataframe
                df_list = [df_list, df_file_data]
                df_list = pd.concat(df_list)
                
                print('procesado OK:',f[1])

            except Exception as e:
                print('error de proceso:',f[1],str(e))


        #================================
        # 4. escribo json con todos los archivos juntos de este tipo de fichero
        json_file_data = path_json_root + 'data'+ tipo_fichero +'.json'
        json_parsed = json.loads(df_list.to_json(orient='values'))
        #borro actual, si ya existe
        if os.path.exists(json_file_data):
            os.remove(json_file_data)
        #escribo nuevo
        with open(json_file_data, 'w', encoding='utf-8') as f:
            json.dump(json_parsed, f, ensure_ascii=False, 
                    indent=None) #revisiones con indent=4

    #################################        

    return 'OK'

