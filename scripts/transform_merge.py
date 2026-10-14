import os
import glob
import pandas as pd

DATA_DIR = "/opt/airflow/data"

def transform_and_merge(**kwargs):
    df_desempleo = pd.read_csv(os.path.join(DATA_DIR, "tasa_desempleo.csv"))
    df_festivos = pd.read_csv(os.path.join(DATA_DIR, "festivos_colombia.csv"))
    df_municipios = pd.read_csv(os.path.join(DATA_DIR, "municipios_valle.csv"))

    df_festivos_anio = df_festivos.groupby('anio').size().reset_index(name='total_festivos_anio')

    archivos_db = glob.glob(os.path.join(DATA_DIR, "db_*.csv"))
    archivo_fact = next((f for f in archivos_db if "fact" in f or "suicidio" in f), archivos_db[0])
    df_base = pd.read_csv(archivo_fact)

    # 1. Unir Dimensiones
    path_tiempo = os.path.join(DATA_DIR, "db_dim_tiempo.csv")
    if os.path.exists(path_tiempo) and 'id_tiempo' in df_base.columns:
        df_tiempo = pd.read_csv(path_tiempo)
        cols_to_use = df_tiempo.columns.difference(df_base.columns).tolist() + ['id_tiempo']
        df_base = df_base.merge(df_tiempo[cols_to_use], on='id_tiempo', how='left')

    path_geo = os.path.join(DATA_DIR, "db_dim_geografia.csv")
    if os.path.exists(path_geo) and 'id_geografia' in df_base.columns:
        df_geo = pd.read_csv(path_geo)
        cols_to_use = df_geo.columns.difference(df_base.columns).tolist() + ['id_geografia']
        df_base = df_base.merge(df_geo[cols_to_use], on='id_geografia', how='left')

    # 2. Cruce con APIs (Limpiando columnas previas si existen)
    col_anio = next((c for c in df_base.columns if c.lower() in ['anio', 'año', 'year']), None)
    if col_anio:
        df_base['anio'] = pd.to_numeric(df_base[col_anio], errors='coerce')
        if 'tasa_desempleo' in df_base.columns: df_base.drop(columns=['tasa_desempleo'], inplace=True)
        if 'total_festivos_anio' in df_base.columns: df_base.drop(columns=['total_festivos_anio'], inplace=True)
        
        df_base = df_base.merge(df_desempleo, on='anio', how='left')
        df_base = df_base.merge(df_festivos_anio, on='anio', how='left')

    # 3. Cruce con Web Scraping
    col_mpio = next((c for c in df_base.columns if 'municipio' in c.lower() and 'codigo' not in c.lower()), None)
    if col_mpio:
        df_base['municipio_norm'] = df_base[col_mpio].astype(str).str.upper().str.strip()
        
        # Eliminar TODAS las columnas residuales de cruces viejos antes de hacer el merge
        cols_residuales = [c for c in df_base.columns if any(x in c for x in ['superficie_km2', 'poblacion_2018', 'altitud_msnm'])]
        if cols_residuales:
            df_base.drop(columns=cols_residuales, inplace=True)
            
        df_base = df_base.merge(
            df_municipios[['municipio_norm', 'superficie_km2', 'poblacion_2018', 'altitud_msnm']],
            on='municipio_norm',
            how='left'
        )

    # 4. Exportar el dataset final
    output_path = os.path.join(DATA_DIR, "dataset_enriquecido_v2.csv")
    df_base.to_csv(output_path, index=False)
    print(f"Dataset enriquecido guardado: {len(df_base)} filas y {len(df_base.columns)} columnas.")
