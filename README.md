# InfoElectoral_INE_TGM - v1
Procesamiento en Python de ficheros .DAT con datos electorales, entregado por el INE de España.
Link de Datos INE: https://infoelectoral.interior.gob.es/opencms/es/elecciones-celebradas/area-de-descargas/

Dos procesos de transformacion:
1) Transformacion de ficheros .DAT a .JSON. Ej: data05.DAT --> data05.JSON
2) Transformacion de ficheros .JSON a: postresql y/o Excel. Ej: data05.JSON --> tbl:data05 y/o data05.XLSX

Incluye:
* JSON con códigos de: Comunidades, Provincias, Municipios
* JSON con cortes de columnas en ficheros .DAT

Requerimientos:
* Descargar ficheros .DAT para ser procesados. Del Link de Datos INE
* Configurar las constantes

