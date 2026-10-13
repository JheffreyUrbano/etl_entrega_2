import os
import pandas as pd
from sqlalchemy import create_engine

DATA_DIR = "/opt/airflow/data"

def load_data_warehouse(**kwargs):
    user = os.getenv("DW_USER", "airflow")
    password = os.getenv("DW_PASSWORD", "airflow")
    host = os.getenv("DW_HOST", "host.docker.internal")
    port = os.getenv("DW_PORT", "5433")
    db = os.getenv("DW_DB", "suicidios_colombia")

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

    tablas_a_cargar = {
        "dim_economia_worldbank": "tasa_desempleo.csv",
        "dim_festivos_nager": "festivos_colombia.csv",
        "dim_municipios_scraping": "municipios_valle.csv",
        "fact_suicidios_enriquecida_v2": "dataset_enriquecido_v2.csv"
    }

    for nombre_tabla, archivo_csv in tablas_a_cargar.items():
        ruta = os.path.join(DATA_DIR, archivo_csv)
        if os.path.exists(ruta):
            df = pd.read_csv(ruta)
            df.to_sql(nombre_tabla, engine, schema="public", if_exists="replace", index=False)
            print(f"Tabla public.{nombre_tabla} cargada ({len(df)} filas).")
