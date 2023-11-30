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
    nombreReferidor
    idReferidor,
    paisResidencia,
    LLENADO_INCORRECTO,
    USUARIOS_ENIDTWOGETHER,
    CANTIDAD_CONMISMOID,
    CASE WHEN USUARIOS_ENIDTWOGETHER > 1 THEN 'CONTACTAR_USUARIO_EN_+2ID_TWOGETHER'
        ELSE 'NO_CONTACTAR' END AS VALIDACION_LLENADO,
    CASE WHEN CANTIDAD_CONMISMOID > 2 THEN 'CONTACTAR_USUARIO_ENID_CON+2_PERSONAS'
        ELSE 'NO_CONTACTAR' END AS VALIDACION_IDTWOGETHER,
    nombreCompleto,
    numeroContacto,
    correoElectronico
FROM USUARIOS_VALIDOS
ORDER BY numeroDocumento, idReferidor;
"""
