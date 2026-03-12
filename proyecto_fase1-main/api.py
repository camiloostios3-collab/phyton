from fastapi import FastAPI
from typing import List
from pathlib import Path

from file_io import read_csv, write_csv
from cleaning import (
    normalize_column_names,
    remove_duplicates,
    drop_empty_rows,
    strip_strings,
)

from schemas import InputPersonaSchema, OutputPersonaSchema

app = FastAPI()

# =========================
# Rutas del proyecto
# =========================
base_path = Path(__file__).resolve().parent
data_path = base_path / "data"

input_file = data_path / "customers_80k_dirty.csv"
output_file = data_path / "customers_clean.csv"


# =========================
# Ruta principal
# =========================
@app.get("/")
def home():
    return {"mensaje": "API funcionando correctamente"}


# =========================
# GET /procesar
# =========================
@app.get("/procesar")
def procesar_csv():

    df = read_csv(input_file)

    df = normalize_column_names(df)
    df = strip_strings(df)
    df = drop_empty_rows(df)
    df = remove_duplicates(df)

    write_csv(df, output_file)

    return {
        "mensaje": "Archivo procesado correctamente",
        "filas_resultado": len(df)
    }


# =========================
# POST /validar
# =========================
@app.post("/validar")
def validar_personas(personas: List[InputPersonaSchema]):

    resultados = []

    for persona in personas:

        salida = OutputPersonaSchema(
            customer_id=persona.customer_id,
            full_name=f"{persona.first_name} {persona.last_name}",
            email=persona.email,
            age=persona.age
        )

        resultados.append(salida.model_dump())

    return {
        "total_recibidos": len(personas),
        "total_validados": len(resultados),
        "datos": resultados
    }