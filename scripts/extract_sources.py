import os
import requests
import pandas as pd
from io import StringIO
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from sqlalchemy import create_engine
from google.cloud import storage

DATA_DIR = "/opt/airflow/data"

def get_http_session():
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def extract_api_worldbank(**kwargs):
    session = get_http_session()
    url = "http://api.worldbank.org/v2/country/CO/indicator/SL.UEM.TOTL.ZS"
    params = {"format": "json", "per_page": 50}
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()[1]
    df_desempleo = pd.DataFrame(data)[['date', 'value']].copy()
    df_desempleo.columns = ['anio', 'tasa_desempleo']
    df_desempleo['anio'] = pd.to_numeric(df_desempleo['anio'])
    df_desempleo = df_desempleo[(df_desempleo['anio'] >= 2015) & (df_desempleo['anio'] <= 2024)]
    df_desempleo = df_desempleo.dropna().reset_index(drop=True)

    output_path = os.path.join(DATA_DIR, "tasa_desempleo.csv")
    df_desempleo.to_csv(output_path, index=False)
    print(f"Desempleo extraído: {len(df_desempleo)} registros.")

def extract_api_holidays(**kwargs):
    session = get_http_session()
    festivos_list = []

    for anio in range(2015, 2025):
        url = f"https://date.nager.at/api/v3/PublicHolidays/{anio}/CO"
        response = session.get(url, timeout=15)
        response.raise_for_status()
        for item in response.json():
            festivos_list.append({
                "fecha": item.get("date"),
                "anio": anio,
                "nombre_festivo": item.get("localName"),
                "es_festivo": True
            })

    df_festivos = pd.DataFrame(festivos_list)
    df_festivos['fecha'] = pd.to_datetime(df_festivos['fecha'])
    df_festivos['mes'] = df_festivos['fecha'].dt.month
    df_festivos['dia'] = df_festivos['fecha'].dt.day

    output_path = os.path.join(DATA_DIR, "festivos_colombia.csv")
    df_festivos.to_csv(output_path, index=False)
    print(f"Festivos extraídos: {len(df_festivos)} registros.")

def extract_web_municipios(**kwargs):
    custom_headers = {"User-Agent": "Mozilla/5.0"}
    url = "https://es.wikipedia.org/wiki/Anexo:Municipios_de_Valle_del_Cauca"
    response = requests.get(url, headers=custom_headers, timeout=15)
    response.raise_for_status()

    tablas = pd.read_html(StringIO(response.text), attrs={'class': 'wikitable'})
    df_municipios = tablas[0].iloc[:, [1, 4, 3, 7, 5]].copy()
    df_municipios.columns = ['municipio', 'superficie_km2', 'poblacion_2018', 'anio_fundacion', 'altitud_msnm']

    for col in ['superficie_km2', 'poblacion_2018', 'anio_fundacion', 'altitud_msnm']:
        df_municipios[col] = df_municipios[col].astype(str).str.replace(r'[^\d.]', '', regex=True)
        df_municipios[col] = pd.to_numeric(df_municipios[col], errors='coerce')

    df_municipios['municipio_norm'] = df_municipios['municipio'].astype(str).str.upper().str.strip()
    output_path = os.path.join(DATA_DIR, "municipios_valle.csv")
    df_municipios.to_csv(output_path, index=False)
    print(f"Municipios extraídos: {len(df_municipios)} registros.")

def extract_gcs_csv(**kwargs):
    bucket_name = os.getenv("GCS_BUCKET_NAME", "ws2-etl-bucket")
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = list(bucket.list_blobs())
    csv_blobs = [b for b in blobs if b.name.lower().endswith('.csv')]

    for blob in csv_blobs:
        dest_file = os.path.join(DATA_DIR, os.path.basename(blob.name))
        blob.download_to_filename(dest_file)
        print(f"Descargado gs://{bucket_name}/{blob.name} -> {dest_file}")

def extract_db_original(**kwargs):
    user = os.getenv("DW_USER", "airflow")
    password = os.getenv("DW_PASSWORD", "airflow")
    host = os.getenv("DW_HOST", "host.docker.internal")
    port = os.getenv("DW_PORT", "5433")
    db = os.getenv("DW_DB", "suicidios_colombia")

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    tablas_db = pd.read_sql(query, engine)['table_name'].tolist()

    for tabla in tablas_db:
        df_tabla = pd.read_sql(f"SELECT * FROM public.{tabla}", engine)
        df_tabla.to_csv(os.path.join(DATA_DIR, f"db_{tabla}.csv"), index=False)
        print(f"Tabla {tabla} exportada ({len(df_tabla)} filas).")
