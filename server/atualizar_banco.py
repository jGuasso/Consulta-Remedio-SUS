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
                INSERT INTO public.medicamento (id_farmacia_digital ,nome , origem_dado, forma_farmaceutica, componente) 
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id_farmacia_digital) DO NOTHING
                RETURNING id;
            """
    componente = 0
    if med.get('elenco')=="EXC":
        componente = 1
    else:
        componente = 2

    cursor.execute(sql_medicamento, (med.get('id'),med.get('nomeComercial'),1,med.get('txtApresentacao'),componente))
    retorno = cursor.fetchone()
    if retorno:
        id_med = retorno[0]
    else:
        sql_select_med = "SELECT id FROM public.medicamento WHERE id_farmacia_digital = %s;"
        cursor.execute(sql_select_med, (med.get('id'),))
        id_med = cursor.fetchone()[0]

    cid_list = med.get('medicamentoCidList')
    for cid in cid_list:
        cid_view = cid.get("cidViewED")
        
        sql_cid = """
                INSERT INTO public.cid (id_farmacia_digital ,codigo ,descricao) 
                VALUES (%s, %s, %s)
                ON CONFLICT (id_farmacia_digital) DO NOTHING
                RETURNING id;
            """
        
        cursor.execute(sql_cid, (cid_view.get("id"),cid_view.get("codigo"),cid_view.get("descricao")))

        retorno = cursor.fetchone()
        if retorno:
            id_cid = retorno[0]
        else:
            sql_select_cid = "SELECT id FROM public.cid WHERE id_farmacia_digital = %s;"
            cursor.execute(sql_select_cid, (cid_view.get('id'),))
            id_cid = cursor.fetchone()[0]

        protocoloClinico_view = cid.get("protocoloClinicoViewED")

        sql_protocolo = """
                    INSERT INTO public.protocolo_clinico (id_farmacia_digital) 
                    VALUES (%s)
                    ON CONFLICT (id_farmacia_digital) DO NOTHING
                    RETURNING id;
                """
        cursor.execute(sql_protocolo,(protocoloClinico_view.get("id"),))

        retorno = cursor.fetchone()
        if retorno:
            id_protocolo = retorno[0]
        else:
            sql_select_protocolo = "SELECT id FROM public.protocolo_clinico WHERE id_farmacia_digital = %s;"
            cursor.execute(sql_select_protocolo, (protocoloClinico_view.get('id'),))
            id_protocolo = cursor.fetchone()[0]

        sql_medicamento_cid_protocolo = """
                    INSERT INTO public.medicamento_cid_protocolo_clinico (id_medicamento, id_cid, id_protocolo_clinico) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id_medicamento, id_cid) DO NOTHING
                """
        cursor.execute(sql_medicamento_cid_protocolo,(id_med,id_cid,id_protocolo))

        documentos_list = protocoloClinico_view.get("protocoloClinicoDocumentoList")
        for doc in documentos_list:
            sql_documento = """
                        INSERT INTO public.documento (nome ,nrointdoc , nrointsubtipodoc) 
                        VALUES (%s, %s, %s)
                        ON CONFLICT (nrointdoc) DO NOTHING
                        RETURNING id; 
                    """
            cursor.execute(sql_documento,(doc.get("nome"),doc.get("nroIntDoc"),doc.get("nroIntSubtipoDocumento")))
            retorno = cursor.fetchone()
            if retorno:
                id_documento = retorno[0]
            else:
                sql_select_documento = "SELECT id FROM public.documento WHERE nrointdoc = %s;"
                cursor.execute(sql_select_documento, (doc.get('nroIntDoc'),))
                id_documento = cursor.fetchone()[0]

            sql_protocolo_clinico_documento = """
                        INSERT INTO public.protocolo_clinico_documento (id_protocolo_clinico , id_documento) 
                        VALUES (%s, %s)
                        ON CONFLICT (id_protocolo_clinico, id_documento) DO NOTHING
                    """

            cursor.execute(sql_protocolo_clinico_documento,(id_protocolo,id_documento))
    connection.commit()
# Close the cursor and connection
cursor.close()
connection.close()
print("Connection closed.")

