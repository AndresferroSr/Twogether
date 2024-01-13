import os
import query
import json
#import gspread
import requests
import pandas as pd
import validators

from datetime import datetime
from pydantic import BaseModel, constr, EmailStr, StrictBool
from pandasql import sqldf

from flask import Flask, jsonify, request, render_template

import settings 

app = Flask(__name__)

port = int(os.environ.get("PORT", 8080))

class DatosFormulario(BaseModel):
    nombreCompleto: constr(strip_whitespace=True, min_length=1)
    paisResidencia: constr(strip_whitespace=True, min_length=1)
    numeroDocumento: constr(strip_whitespace=True, min_length=1)
    numeroContacto: constr(strip_whitespace=True, min_length=1)
    correoElectronico: EmailStr
    fechaNacimiento: str
    nombreReferidor: constr(strip_whitespace=True, min_length=1)
    idReferidor: constr(strip_whitespace=True, min_length=1)

def transform_phone_number(phone_number, country):
    # Obtener el código de país
    country_code = query.COUNTRIES.get(country, '')
    # Verificar si el número ya tiene el código de país
    if phone_number.startswith(country_code):
        return phone_number
    else:
        return f'{country_code}{phone_number}'

def validar_calcular_edad(fecha_nacimiento):
    # Convertir la cadena de fecha a un objeto datetime
    fecha_nacimiento = datetime.strptime(fecha_nacimiento, '%Y-%m-%d')
    fecha_actual = datetime.now()
    edad = fecha_actual.year - fecha_nacimiento.year - ((fecha_actual.month, fecha_actual.day) < (fecha_nacimiento.month, fecha_nacimiento.day))
    if edad < 18:
        raise ValueError("La persona es menor de edad.")
    return edad

def validar_longitud_celular(phone_number, country):
    # Obtener el código de país
    country_length = query.LENGTH_COUNTRIES.get(country, '')
    # Verificar si el número ya tiene el código de país

    if len(phone_number) == country_length:
        return phone_number
    else:
        raise ValueError("Longitud de numero de celular incorrecta segun tu pais, por favor, revisa")

@app.route('/getuser', methods = ["GET", "POST"])
def get_user():
    user_id = request.args.get('userID')
    frame_result = settings.client.query(f"select nombreCompleto, numeroDocumento from web_page.form_web_llenado WHERE numeroDocumento = '{user_id}'").to_dataframe()

    if frame_result.shape[0] >= 1:
        print(frame_result.shape[0])
        return jsonify({'status': 'success', 
                                'code': 200,
                                'user': frame_result.tail(1).to_dict(orient = "records"),
                                'message': 'Formulario recibido correctamente'})
    return jsonify({'status': 'error', 
                    'code': 400,
                    'message': 'Usuario no encontrado en la base de datos'})
        


@app.route('/front', methods = ["GET", "POST"])
def front():
    if request.method == "POST":
        try:
            # dict_form = request.form.to_dict()
            dict_form = json.loads(request.data)

            valid_compra = settings.client.query(query.QUERY_COMPRA).to_dataframe()
            
            dict_form["pasosCumplidos"] = str(dict_form["pasosCumplidos"])

            validar_longitud_celular(dict_form['numeroContacto'],  dict_form['paisResidencia'])

            dict_form['numeroContacto'] = transform_phone_number(dict_form['numeroContacto'], dict_form['paisResidencia'])
            
            validar_calcular_edad(dict_form["fechaNacimiento"])

            datos_formulario = DatosFormulario(**dict_form)
            frame = pd.DataFrame(dict_form, index=[0])
            frame['fechaRegistro'] = pd.to_datetime(datetime.now())

            frame.to_gbq(destination_table = f'{settings.DATASET}.form_web_llenado',
                                project_id = settings.PROJECT_ID,
                                credentials = settings.credentials,
                                if_exists = "append")

            allframe = settings.client.query("select * from web_page.form_web_llenado").to_dataframe()
            frame_corregido = sqldf(query.CORRECION_LLENADO)
            frame_contactos = sqldf(query.CONTACTOS)
            frame_contactos["fechaRegistro"] = pd.to_datetime(frame_contactos["fechaRegistro"])
            
            filtro = (
                (frame_contactos['fechaRegistro'] == frame['fechaRegistro'].iloc[0]) &
                (frame_contactos['numeroDocumento'] == frame['numeroDocumento'].iloc[0])# &
            )
            
            frame_contactos_filtrado = frame_contactos[filtro]

            frame_contactos_filtrado['primerNombre'] = frame_contactos_filtrado['nombreCompleto'].apply(validators.obtener_primer_nombre)
            frame_contactos_filtrado_serial = validators.correct_frame(frame_contactos_filtrado).to_dict(orient = "records")
            frame_contactos_filtrado_serial_valid = frame_contactos_filtrado_serial[0]
            
            if frame_contactos_filtrado_serial_valid.get('numeroDocumento').startswith("LC"):
                frame_contactos_filtrado_serial["referidosPorReferidor"] = 0
                frame_contactos_filtrado_serial["registroConMismoId"] = 0
                pass # como es lider de comunidad, no deben correr las validaciones
            else:
                if int(frame_contactos_filtrado_serial_valid.get("referidosPorReferidor")) > 2:
                    settings.client.query(f"delete from web_page.form_web_llenado WHERE numeroDocumento = '{frame_contactos_filtrado_serial_valid.get('numeroDocumento')}'")

                if int(frame_contactos_filtrado_serial_valid.get("registroConMismoId")) > 1:
                    frame = settings.client.query(f"""SELECT numeroDocumento, fechaRegistro,
                                row_number() over(partition by numeroDocumento order by fechaRegistro asc) as rown
                                FROM towgether.web_page.form_web_llenado
                                where numeroDocumento = '{frame_contactos_filtrado_serial_valid.get('numeroDocumento')}'
                                qualify rown = 2
                                """).to_dataframe()
                    deletes = frame.to_dict(orient = "records")[0]
                    settings.client.query(f"""delete 
                                FROM `towgether.web_page.form_web_llenado` 
                                where numeroDocumento = '{deletes['numeroDocumento']}'
                                and fechaRegistro = '{deletes['fechaRegistro']}'
                    """)
            requests.post('https://hook.us1.make.com/di6tg4ufk8hx9s2tc977tlj7pinsua38', json = frame_contactos_filtrado_serial)


            frame_valid_compra = settings.client.query(query.QUERY_COMPRA).to_dataframe()

            if dict_form["idReferidor"] in frame_valid_compra["idReferidor"].values:
                
                settings.client.query(f"""delete 
                                          from towgether.web_page.form_web_disparador_compra 
                                          where idReferidor = '{frame_valid_compra["idReferidor"].unique()[0]}' """)
                
                frame_valid_compra.to_gbq(destination_table = f'{settings.DATASET}.form_web_disparador_compra',
                                            project_id = settings.PROJECT_ID,
                                            credentials = settings.credentials,
                                            if_exists = "append")
                
                serializador = [{
                    'ID_REFERIDOR_COMPRA': frame_valid_compra["idReferidor"].unique()[0],
                    'NOMBRE_REFERIDOR_COMPRA': frame_valid_compra["nombreReferidor"].unique()[0],
                    'ID_REFERIDO_1': frame_valid_compra["numeroDocumento"].values[0],
                    'NOMBRE_REFERIDO_1': frame_valid_compra["nombreCompleto"].values[0],
                    'ID_REFERIDO_2': frame_valid_compra["numeroDocumento"].values[1],
                    'NOMBRE_REFERIDO_2': frame_valid_compra["nombreCompleto"].values[1]
                }]

                print("Hacer la Peticion a servidor")
                                
            return jsonify({'status': 'success', 
                            'code': 200,
                            'user': validators.correct_frame(frame_contactos_filtrado).to_dict(orient = "records"),
                            'message': 'Formulario recibido correctamente'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': 
                            f'Error en los datos del formulario: {str(e)}',
                            'code': 400})
        
    return render_template('index.html')


# Ejecutar la aplicación Flask
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=port)