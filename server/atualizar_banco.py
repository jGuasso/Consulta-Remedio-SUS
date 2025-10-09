import json
import requests
import time
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

#Headers
headers = {
    "Accept": os.getenv("HEADER_ACCEPT"),
    "Accept-Language": os.getenv("HEADER_ACCEPT_LANGUAGE"),
    "Client_id": os.getenv("HEADER_CLIENT_ID"),
    "Connection": os.getenv("HEADER_CONNECTION"),
    "Origin": os.getenv("HEADER_ORIGIN"),
    "Referer": os.getenv("HEADER_REFERER"),
    "User-Agent": os.getenv("HEADER_USER_AGENT")
}

# URL base para buscar todos os medicamentos
url_busca: str = os.getenv("URL_BUSCA")

# URL base de consulta de medicamento
url_consulta: str = os.getenv("URL_CONSULTA")

# Credenciais do banco de dados
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST")
PORT = os.getenv("DB_PORT")
DBNAME = os.getenv("DB_NAME")

#Requisição Lista de Medicamentos
medicamentos = None
try:
    response = requests.get(url_busca,headers=headers)
    if response.status_code == 200:
        medicamentos = response.json()
        print("Lista de medicamentos carregada com sucesso")
    else:
        print(f"Lista de medicamento; Status code:{response.status_code}")
        exit()
except requests.exceptions.RequestException as e:
    print(f"Erro na requisição da lista de medicamentos: {e}")
    exit()

connection = None
cursor = None
try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("Connection successful!")
    
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()

except Exception as e:
    print(f"Failed to connect: {e}")
    exit()

for m in medicamentos:
    med_id = m.get('id')
    if not med_id:
        print(f"Pulando medicamento pois não possui ID: {m}")
        continue
    url = f"{url_consulta}{med_id}"
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            med = response.json()
            print(f"Medicamento {med_id} Carregado")
        else: 
            print(f"Medicamento {med_id}; Status code:{response.status_code}")
            continue
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição da lista de medicamentos: {e}")
        continue
    
    sql_medicamento = """
                INSERT INTO public.medicamento (id_farmacia_digital ,nome , origem_dado, forma_farmaceutica) 
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id_farmacia_digital) DO NOTHING
                RETURNING id;
            """
    
    cursor.execute(sql_medicamento, (med.get('id'),med.get('nomeComercial'),1,med.get('txtApresentacao')))

# Close the cursor and connection
connection.commit()
cursor.close()
connection.close()
print("Connection closed.")

