/*
----------------------------------------
SCRIPTS PARA AJUSTAR LA BASE DE DATOS

AL 20230301:
* EJECUCION MANUAL CONTRA POSTGRES
----------------------------------------
*/

-----------
--INDICES

drop index IF EXISTS data03_idx;
drop index IF EXISTS data04_idx;
drop index IF EXISTS data06_idx;
drop index IF EXISTS data08_idx;
drop index IF EXISTS data10_idx;

CREATE UNIQUE INDEX data03_idx ON data03 (id_candidatura);
CREATE UNIQUE INDEX data04_idx ON data04 (id_provincia, id_municipio, id_distrito, id_candidatura, "Candidato_Orden");
CREATE UNIQUE INDEX data06_idx ON data06 (id_provincia, id_municipio, id_distrito, id_candidatura);
CREATE UNIQUE INDEX data08_idx ON data08 (id_comunidad, id_provincia, id_municipio, id_candidatura);
CREATE UNIQUE INDEX data10_idx ON data10 (id_comunidad, id_provincia, id_municipio, id_distrito, id_candidatura, "CodigoSeccion", "CodigoMesa");

-----------
--NUEVA COLUMNA CON LA CONCATENACION DE COM+PRO+MUN EN 1 SOLA COLUMNA

alter table data10 add id_geo bigint null

update data10
set id_geo = cast(concat_ws('', CAST(id_comunidad AS varchar(3)), CAST(id_provincia AS varchar(3)), CAST(id_municipio AS varchar(3))) as bigint)

--

alter table df_ubicaciones add id_geo bigint null

update df_ubicaciones
set id_geo = cast(concat_ws('', CAST(id_comunidad AS varchar(3)), CAST(id_provincia AS varchar(3)), CAST(id_municipio AS varchar(3))) as bigint)

-----------

--AJUSTES POR MALA CALIDAD DEL DATO (BIEN LA PROVINCIA, PERO MAL LA COMUNIDAD!)
--data08: id_comunidad
WITH comunidades AS (
    select p.id_comunidad, p.id_provincia
	from  df_provincia as p
)
UPDATE data08
SET id_comunidad = comunidades.id_comunidad
FROM comunidades
WHERE data08.id_provincia = comunidades.id_provincia;

--AJUSTES POR MALA CALIDAD DEL DATO (BIEN LA PROVINCIA, PERO MAL LA COMUNIDAD!)
--data10: id_comunidad
WITH comunidades AS (
    select p.id_comunidad, p.id_provincia
	from  df_provincia as p
)
UPDATE data10
SET id_comunidad = comunidades.id_comunidad
FROM comunidades
WHERE data10.id_provincia = comunidades.id_provincia;

