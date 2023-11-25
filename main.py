import os
import json
import query
import gspread
import requests

import pandas as pd
from pandasql import sqldf

from flask import Flask, jsonify, request

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


@app.route('/api/data', methods=['GET'])
def get_data():
    frame = read_bad_excel(FILE)
    frame_corregido = sqldf(query.CORRECION_LLENADO)
    frame_contactos = sqldf(query.CONTACTOS)
    json_data = frame_contactos.to_json(orient='records', default_handler = str)
    return jsonify({'data': json_data})


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

# Ejecutar la aplicación Flask
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=port)