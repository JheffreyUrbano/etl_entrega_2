import sys
import os

# carpeta raíz de Airflow dentro del contenedor
sys.path.insert(0, '/opt/airflow')

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# importar funciones de extracción, transformación, validación y carga
from scripts.extract_sources import (
    extract_api_worldbank,
    extract_api_holidays,
    extract_web_municipios,
    extract_gcs_csv,
    extract_db_original
)
from scripts.transform_merge import transform_and_merge
from scripts.validate_ge import validate_data_quality
from scripts.load_dw import load_data_warehouse

default_args = {
    'owner': 'alexander_jheffrey',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='etl_suicidios_valle_v2',
    default_args=default_args,
    description='Pipeline ETL Segunda Entrega con Cuarentena de Datos',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'entrega2', 'salud_mental']
) as dag:

    t_ext_wb = PythonOperator(task_id='extract_api_worldbank', python_callable=extract_api_worldbank)
    t_ext_hol = PythonOperator(task_id='extract_api_holidays', python_callable=extract_api_holidays)
    t_ext_web = PythonOperator(task_id='extract_web_municipios', python_callable=extract_web_municipios)
    t_ext_gcs = PythonOperator(task_id='extract_gcs_csv', python_callable=extract_gcs_csv)
    t_ext_db = PythonOperator(task_id='extract_db_original', python_callable=extract_db_original)

    t_transform = PythonOperator(task_id='transform_and_merge', python_callable=transform_and_merge)
    t_validate = PythonOperator(task_id='validate_data_quality', python_callable=validate_data_quality)
    t_load = PythonOperator(task_id='load_data_warehouse', python_callable=load_data_warehouse)

    # Orden de dependencias
    [t_ext_wb, t_ext_hol, t_ext_web, t_ext_gcs, t_ext_db] >> t_transform >> t_validate >> t_load
