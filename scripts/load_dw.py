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

    # 1. Cargar tablas dimensionales auxiliares
    tablas_dim = {
        "dim_economia_worldbank": "tasa_desempleo.csv",
        "dim_festivos_nager": "festivos_colombia.csv",
        "dim_municipios_scraping": "municipios_valle.csv"
    }
    for nombre, archivo in tablas_dim.items():
        ruta = os.path.join(DATA_DIR, archivo)
        if os.path.exists(ruta):
            df = pd.read_csv(ruta)
            df.to_sql(nombre, engine, schema="public", if_exists="replace", index=False)
            print(f"Dimensión public.{nombre} cargada exitosamente.")

    # 2. Cargar tabla de hechos LIMPIA
    ruta_limpia = os.path.join(DATA_DIR, "dataset_limpio.csv")
    if os.path.exists(ruta_limpia):
        df_limpio = pd.read_csv(ruta_limpia)
        df_limpio.to_sql("fact_suicidios_enriquecida_v2", engine, schema="public", if_exists="replace", index=False)
        print(f"ÉXITO: {len(df_limpio)} registros validados cargados en public.fact_suicidios_enriquecida_v2.")

    # 3. Cargar tabla de CUARENTENA
    ruta_cuarentena = os.path.join(DATA_DIR, "dataset_cuarentena.csv")
    if os.path.exists(ruta_cuarentena):
        df_cuarentena = pd.read_csv(ruta_cuarentena)
        if not df_cuarentena.empty:
            df_cuarentena.to_sql("cuarentena_suicidios", engine, schema="public", if_exists="replace", index=False)
            print(f"ATENCIÓN: {len(df_cuarentena)} registros anómalos enviados a public.cuarentena_suicidios.")
        else:
            print("No hubo registros en cuarentena. Calidad de datos perfecta.")
