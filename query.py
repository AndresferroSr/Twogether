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
