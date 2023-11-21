from flask import Flask
import requests

app = Flask(__name__)

@app.route('/realizar-peticion', methods=['POST'])
def realizar_peticion():
    # Datos que quieres enviar en la petición POST
    datos = {'parametro1': 'valor1', 'parametro2': 'valor2'}

    # Ruta a la que se enviará la petición POST
    url_destino = 'https://hook.us1.make.com/di6tg4ufk8hx9s2tc977tlj7pinsua38'

    # Realizar la petición POST utilizando la biblioteca requests
    respuesta = requests.post(url_destino, data=datos)

    # Devolver la respuesta al cliente
    return respuesta.text, respuesta.status_code

if __name__ == '__main__':
    app.run(debug=True)