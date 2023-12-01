import os
import query
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

FILE = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQixwgy_oJiSYDHhcESEAoIOKN5KXCFEPO6LBCMAnkaYQh8X0EqJWrIv7vi7uceqaYPGfEkHx_acsey/pub?output=csv"

meses = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
    'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
    'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
}

port = int(os.environ.get("PORT", 8080))


def read_bad_excel(path):
    gc = gspread.service_account("keys.json")  
    df = pd.read_csv(path, header=None)
    df.columns = df.iloc[1]
    df = df[2:]
    df.reset_index(drop=True, inplace=True)
    
    df["hora_convertida"] = pd.to_datetime(df["HORA_DE_REGISTRO"], format='%I:%M %p').dt.time
    df["dia"] = df["FECHAREGISTRO"].str.split(" ")[0][0]
    df["mes"] = df["FECHAREGISTRO"].str.split(" ")[0][1].replace(",", "")
    df["year"] = df["FECHAREGISTRO"].str.split(" ")[0][2]

    df['fecha_hora'] = pd.to_datetime(
        df['year'].astype(str) + '-' +
        df['mes'].map(meses).astype(str) + '-' +
        df['dia'].astype(str) + ' ' +
        df['hora_convertida'].astype(str)
    )
    return df


@app.route('/hook', methods=['GET'])
def hook_data():
    frame = read_bad_excel(FILE)
    frame_corregido = sqldf(query.CORRECION_LLENADO)
    frame_contactos = sqldf(query.CONTACTOS)
    jsondata = frame_contactos.to_dict(orient = "records")

    for i in range(0, len(jsondata)):
        response = requests.post('https://hook.us1.make.com/di6tg4ufk8hx9s2tc977tlj7pinsua38', 
                                json = frame_contactos.to_dict(orient = "records",)[i]
        ) 
        print({'status': response.status_code, 'message': response.text})
    return jsonify({'status': response.status_code,
                    'message': response.text})


class DatosFormulario(BaseModel):
    nombreCompleto: constr(strip_whitespace=True, min_length=1)
    paisResidencia: constr(strip_whitespace=True, min_length=1)
    numeroDocumento: constr(strip_whitespace=True, min_length=1)
    numeroContacto: constr(strip_whitespace=True, min_length=1)
    correoElectronico: EmailStr
    fechaNacimiento: str
    nombreReferidor: constr(strip_whitespace=True, min_length=1)
    idReferidor: constr(strip_whitespace=True, min_length=1)

@app.route('/front', methods = ["GET", "POST"])
def front():
    if request.method == "POST":
        try:
            dict_form = request.form.to_dict()

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
            frame_contactos_filtrado_serial = validators.correct_frame(frame_contactos_filtrado)
            frame_contactos_filtrado_serial

            requests.post('https://hook.us1.make.com/di6tg4ufk8hx9s2tc977tlj7pinsua38', json = frame_contactos_filtrado_serial.to_dict(orient = "records"))

            return jsonify({'status': 'success', 
                            'code': 200,
                            'user': validators.correct_frame(frame_contactos_filtrado).to_dict(orient = "records"),
                            'message': 'Formulario recibido correctamente'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': 
                            f'Error en los datos del formulario: {str(e)}'})
    return render_template('index.html')


# Ejecutar la aplicación Flask
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=port)