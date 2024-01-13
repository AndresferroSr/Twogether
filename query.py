CORRECION_LLENADO = """
SELECT
    fechaRegistro,
    nombreCompleto,
    numeroDocumento,
    numeroContacto,
    correoElectronico,
    fechaNacimiento,
    paisResidencia,
    nombreReferidor,
    idReferidor,
    pasosCumplidos,
    ROW_NUMBER() OVER (PARTITION BY numeroDocumento, idReferidor ORDER BY fechaRegistro desc) AS LLENADO_INCORRECTO,
    ROW_NUMBER() OVER (PARTITION BY numeroDocumento ORDER BY fechaRegistro) AS USUARIOS_ENIDTWOGETHER
FROM allframe
"""

CONTACTOS = """

WITH USUARIOS_VALIDOS AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY idReferidor ORDER BY fechaRegistro) AS CANTIDAD_CONMISMOID
    FROM frame_corregido
    WHERE LLENADO_INCORRECTO = 1
)

SELECT 
    fechaRegistro,
    numeroDocumento,
    nombreReferidor,
    idReferidor,
    paisResidencia,
    USUARIOS_ENIDTWOGETHER as registroConMismoId,
    CANTIDAD_CONMISMOID as referidosPorReferidor,
    nombreCompleto,
    numeroContacto,
    correoElectronico
FROM USUARIOS_VALIDOS
ORDER BY numeroDocumento, idReferidor;
"""

COUNTRIES = {
    'Japan': '81',
    'Hong Kong': '852',
    'Singapore': '65',
    'Cambodia': '855',
    'Philippines': '63',
    'Malaysia': '60',
    'Thailand': '66',
    'Australia': '61',
    'Indonesia': '62',
    'Mainland': '86',
    'Taiwan': '886',
    'India': '91',
    'New Zealand': '64',
    'Kazakhstan': '7',
    'Kyrgyzstan': '996',
    'Mongolia': '976',
    'Uzbekistan': '998',
    'Russia': '7',
    'Turkey': '90',
    'United Kingdom': '44',
    'Mexico': '52',
    'Ecuador': "593",
    'CO': '57',
    'Brasil': '55',
    'EEUU': '1',
    'Canada': '1',
    'Alemania': '49',
    'Bélgica': '32',
    'Croacia': '385',
    'Dinamarca': '45',
    'España': '34',
    'Francia': '33',
    'Irlanda': '352',
    'Letonia': '371',
    'Luxemburgo': '352',
    'Países Bajos': '31',
    'Suecia': '46',
    'Bulgaria': '359',
    'Eslovaquia': '421',
    'Estonia': '372',
    'Grecia': '30',
    'Malta': '356',
    'Polonia': '48',
    'República Checa': '420',
    'Austria': '43',
    'Chipre': '357',
    'Eslovenia': '386',
    'Finlandia': '358',
    'Hungría': '36',
    'Italia': '39',
    'Lituania': '370',
    'Portugal': '351',
    'Rumania': '40',
    'Peru': '51',
    "Chile": '56'
}


LENGTH_COUNTRIES  = {
    'Japan': 2,
    'Hong Kong': 8,
    'Singapore': 8,
    'Cambodia': 9,
    'Philippines': 10,
    'Malaysia': 9,
    'Thailand': 9,
    'Australia': 9,
    'Indonesia': 10,
    'Mainland': 11,
    'Taiwan': 9,
    'India': 10,
    'New Zealand': 10,
    'Kazakhstan': 10,
    'Kyrgyzstan': 9,
    'Mongolia': 8,
    'Uzbekistan': 9,
    'Russia': 11,
    'Turkey': 10,
    'United Kingdom': 11,
    'Mexico': 10,
    'Ecuador': 10,
    'CO': 10,
    'Brasil': 10,
    'EEUU': 10,
    'Canada': 10,
    'Alemania': 11,
    'Bélgica': 9,
    'Croacia': 8,
    'Dinamarca': 8,
    'España': 9,
    'Francia': 9,
    'Irlanda': 9,
    'Letonia': 8,
    'Luxemburgo': 9,
    'Países Bajos': 10,
    'Suecia': 9,
    'Bulgaria': 9,
    'Eslovaquia': 9,
    'Estonia': 8,
    'Grecia': 10,
    'Malta': 8,
    'Polonia': 9,
    'República Checa': 9,
    'Austria': 8,
    'Chipre': 8,
    'Eslovenia': 8,
    'Finlandia': 10,
    'Hungría': 9,
    'Italia': 9,
    'Lituania': 8,
    'Portugal': 9,
    'Rumania': 8,
    'Peru': 10,
    'Chile': 8
}


QUERY_COMPRA = '''



with test as 
(
SELECT 
    nombreReferidor, idReferidor, count(*) as cuenta 
FROM `towgether.web_page.form_web_llenado`
group by 1,2
having cuenta > 1
),

interm as (
    select test.*, pa.numeroDocumento, pa.nombreCompleto
from test
inner join `towgether.web_page.form_web_llenado` pa
on test.idReferidor = pa.idReferidor
),

validos as (
select 
    interm.nombreReferidor,
    interm.idReferidor,
    interm.cuenta,
    interm.numeroDocumento,
    interm.nombreCompleto,
    tt.cuenta as valid
from interm
left join test tt
on interm.numeroDocumento = tt.idReferidor
)

select 
    idReferidor, 
    nombreReferidor, 
    numeroDocumento, 
    nombreCompleto, sum(valid) as valid
from validos
group by 1,2,3,4
having valid = 2
and idReferidor not in (select distinct idReferidor from towgether.web_page.form_web_disparador_compra)


'''