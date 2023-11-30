import os
from google.oauth2 import service_account
from google.cloud import bigquery

PROJECT_ID = os.getenv('PROJECT_ID', 'towgether')
DATASET = os.getenv('DATASET', 'web_page')
RUTA = os.getenv('RUTA', r"C:\Users\ASUS\Documents\Programacion\pruebasTecnicas\mercadoLibre\meli\data\BBDD.xlsx")
KEYS = os.getenv('KEYS', r"./keys.json")

credentials = service_account.Credentials.from_service_account_file(f'{KEYS}')
client = bigquery.Client(credentials = credentials)
