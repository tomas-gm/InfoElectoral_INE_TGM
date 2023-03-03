#################################################
"""
#--------------------------------------------------
./CONFIG/CONST.PY
    CORREGIR LOS DIRECTORIOS Y/O EL SERVER POSTGRES
#--------------------------------------------------
"""
#################################################
 
 

#################################################
import sys
sys.path.append('etl')
from etl import dat2json, json2other
#################################################



#################################################
#llamado para procesar todos los archivos .DAT
resultado_dat2json = ''
try:
    resultado_dat2json = dat2json.dat2json()
    print(resultado_dat2json)
except Exception as e:
    print('Error en el proceso dat2json():',str(e))
#################################################



#################################################
#llamado para procesar todos los archivos .DAT
resultado_json2other = ''
try:
    bool_DoSql = True   # True=Crea tablas en server postgresql. False=no hace nada
    bool_DoXls = True   # True=Crea archivos excel (xlsx). Demora varios minutos. False=no hace nada
    resultado_json2other = json2other.json2other(bool_DoSql, bool_DoXls)
    print(resultado_json2other)
except Exception as e:
    print('Error en el proceso json2other('+ str(bool_DoSql) +', '+ str(bool_DoXls) +'):',str(e))
#################################################







