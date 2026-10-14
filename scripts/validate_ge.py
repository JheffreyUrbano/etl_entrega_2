import os
import pandas as pd
import great_expectations as ge

DATA_DIR = "/opt/airflow/data"

def validate_data_quality(**kwargs):
    input_path = os.path.join(DATA_DIR, "dataset_enriquecido_v2.csv")
    df = pd.read_csv(input_path)
    ge_df = ge.from_pandas(df)

    # Set para almacenar los índices de las filas que no cumplan las reglas
    bad_indices = set()
    ge_format = {"result_format": "COMPLETE"} # Necesario para que GE devuelva los índices

    # Regla 1: Rango de año válido y no nulo
    if "anio" in df.columns:
        res_null = ge_df.expect_column_values_to_not_be_null("anio", **ge_format)
        if not res_null["success"]:
            bad_indices.update(res_null["result"]["unexpected_index_list"])
            
        res_rango = ge_df.expect_column_values_to_be_between("anio", min_value=2015, max_value=2025, **ge_format)
        if not res_rango["success"]:
            bad_indices.update(res_rango["result"]["unexpected_index_list"])

    # Regla 2: Tasa de desempleo lógica
    if "tasa_desempleo" in df.columns:
        res_desemp = ge_df.expect_column_values_to_be_between("tasa_desempleo", min_value=0.0, max_value=50.0, **ge_format)
        if not res_desemp["success"]:
            bad_indices.update(res_desemp["result"]["unexpected_index_list"])
            
    # Regla 3: Dominio cerrado para sexo (ejemplo)
    if "sexo" in df.columns:
        res_sexo = ge_df.expect_column_values_to_be_in_set("sexo", ["MASCULINO", "FEMENINO"], **ge_format)
        if not res_sexo["success"]:
            bad_indices.update(res_sexo["result"]["unexpected_index_list"])

    # Separar los datos en Limpios y Cuarentena
    if bad_indices:
        bad_indices_list = list(bad_indices)
        df_cuarentena = df.loc[bad_indices_list]
        df_limpio = df.drop(index=bad_indices_list)
    else:
        df_cuarentena = pd.DataFrame(columns=df.columns)
        df_limpio = df

    # Guardar ambos datasets para la fase de carga
    df_limpio.to_csv(os.path.join(DATA_DIR, "dataset_limpio.csv"), index=False)
    df_cuarentena.to_csv(os.path.join(DATA_DIR, "dataset_cuarentena.csv"), index=False)

    print("=== REPORTE DE CALIDAD (GREAT EXPECTATIONS) ===")
    print(f"Total procesados: {len(df)}")
    print(f"Registros válidos (Aprobados): {len(df_limpio)}")
    print(f"Registros en cuarentena (Rechazados): {len(df_cuarentena)}")

    # Solo detener el pipeline si ABSOLUTAMENTE TODOS los datos son malos
    if len(df_limpio) == 0:
        raise ValueError("Todos los registros fallaron las pruebas de calidad. Pipeline detenido.")
