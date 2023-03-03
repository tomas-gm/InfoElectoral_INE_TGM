####################################
#root de los datos

# directorio root de la app
conf_path_root = '.'

# directorio donde se guardaran los datos estaticos utilizados (ej. json de ubicaciones)
conf_path_static_root = conf_path_root + '/data_static/'

# directorio donde estaran las carpetas con los ficheros .DAT
conf_path_data_root = conf_path_root + '/data_ine/'

# directorio donde se guardaran las transformaciones en ficheros finales JSON
conf_path_json_root = conf_path_root + '/data_ine_json/'

# directorio donde se guardaran las transformaciones en ficheros finales XLSX
conf_path_xlsx_root = conf_path_root + '/data_ine_xlsx/'

####################################


####################################
#postgresql engine string
conf_postgres_engine_str = 'postgresql://postgres:postgres@127.0.0.1:5432/elecciones'
####################################