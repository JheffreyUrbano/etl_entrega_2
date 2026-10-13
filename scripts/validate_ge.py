import os
import pandas as pd
import great_expectations as ge

DATA_DIR = "/opt/airflow/data"

def validate_data_quality(**kwargs):
    input_path = os.path.join(DATA_DIR, "dataset_enriquecido_v2.csv")
    df = pd.read_csv(input_path)
    ge_df = ge.from_pandas(df)

    resultados = []
    r1 = ge_df.expect_table_row_count_to_be_between(min_value=1)
    resultados.append(("Conteo de filas > 0", r1["success"]))

    if "anio" in df.columns:
        r2 = ge_df.expect_column_values_to_not_be_null("anio")
        r3 = ge_df.expect_column_values_to_be_between("anio", min_value=2015, max_value=2025)
        resultados.append(("Anio sin nulos", r2["success"]))
        resultados.append(("Rango de anio (2015-2025)", r3["success"]))

    if "tasa_desempleo" in df.columns:
        r4 = ge_df.expect_column_values_to_be_between("tasa_desempleo", min_value=0.0, max_value=50.0)
        resultados.append(("Tasa de desempleo en rango (0-50%)", r4["success"]))

    fallos = [nombre for nombre, exito in resultados if not exito]
    if fallos:
        raise ValueError(f"Fallo en Great Expectations: {fallos}")
    print("Validación de Great Expectations exitosa.")
