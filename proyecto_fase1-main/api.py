from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from pathlib import Path
import pandas as pd
import io

from file_io import read_csv, write_csv
from cleaning import (
    normalize_column_names,
    remove_duplicates,
    drop_empty_rows,
    strip_strings,
    remove_invalid_emails,
    create_full_name,
    create_is_adult,
)
from schemas import InputPersonaSchema, OutputPersonaSchema

app = FastAPI()

# =========================
# CORS — permite que el
# dashboard HTML llame a
# la API sin ser bloqueado
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# Rutas del proyecto
# =========================
base_path   = Path(__file__).resolve().parent
data_path   = base_path / "data"
input_file  = data_path / "customers_80k_dirty.csv"
output_file = data_path / "customers_clean.csv"


# =========================
# GET /
# =========================
@app.get("/")
def home():
    return {"mensaje": "API funcionando correctamente"}


# =========================
# GET /procesar
# Limpia el CSV fijo en disco
# con el pipeline completo
# =========================
@app.get("/procesar")
def procesar_csv():

    df = read_csv(input_file)

    df = normalize_column_names(df)
    df = strip_strings(df)
    df = remove_duplicates(df)
    df = drop_empty_rows(df)
    df = remove_invalid_emails(df)
    df = create_full_name(df)
    df = create_is_adult(df)

    write_csv(df, output_file)

    return {
        "mensaje": "Archivo procesado correctamente",
        "filas_resultado": len(df),
    }


# =========================
# POST /validar
# Valida una lista de personas
# =========================
@app.post("/validar")
def validar_personas(personas: List[InputPersonaSchema]):

    resultados = []

    for persona in personas:
        salida = OutputPersonaSchema(
            customer_id=persona.customer_id,
            full_name=f"{persona.first_name} {persona.last_name}",
            email=persona.email,
            age=persona.age,
        )
        resultados.append(salida.model_dump())

    return {
        "total_recibidos": len(personas),
        "total_validados": len(resultados),
        "datos": resultados,
    }


# =========================
# POST /upload
# Recibe cualquier CSV desde
# el dashboard y devuelve
# estadísticas en JSON
# =========================
@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):

    contents = await file.read()
    df = pd.read_csv(io.BytesIO(contents))

    nulos      = {k: int(v) for k, v in df.isnull().sum().items()}
    duplicados = int(df.duplicated().sum())
    tipos      = {col: str(dtype) for col, dtype in df.dtypes.items()}

    desc_raw    = df.describe(include="all").fillna("").to_dict()
    descripcion = {
        col: {k: str(v) for k, v in stats.items()}
        for col, stats in desc_raw.items()
    }

    return {
        "nombre_archivo":   file.filename,
        "filas":            len(df),
        "columnas":         len(df.columns),
        "nombres_columnas": list(df.columns),
        "tipos":            tipos,
        "nulos":            nulos,
        "duplicados":       duplicados,
        "descripcion":      descripcion,
    }


# =========================
# Helper: limpiar df genérico
# Se reutiliza en /correlacion
# /outliers y /datos
# =========================
def _limpiar_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip().str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w]", "", regex=True)
    )
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    df = df.dropna()
    df = df.drop_duplicates()
    return df


# =========================
# POST /correlacion
# Devuelve la matriz de
# correlación de columnas
# numéricas (base limpia)
# =========================
@app.post("/correlacion")
async def correlacion_csv(file: UploadFile = File(...)):

    contents = await file.read()
    df = _limpiar_df(pd.read_csv(io.BytesIO(contents)))

    num_df = df.select_dtypes(include=["int64", "float64"])

    if num_df.empty or len(num_df.columns) < 2:
        return {"error": "No hay suficientes columnas numéricas para calcular correlación"}

    corr    = num_df.corr().round(4)
    columnas = list(corr.columns)
    matriz   = corr.values.tolist()

    return {"columnas": columnas, "matriz": matriz}


# =========================
# POST /outliers
# Detecta outliers por IQR
# en columnas numéricas
# (base limpia)
# =========================
@app.post("/outliers")
async def outliers_csv(file: UploadFile = File(...)):

    contents = await file.read()
    df = _limpiar_df(pd.read_csv(io.BytesIO(contents)))

    num_df = df.select_dtypes(include=["int64", "float64"])

    if num_df.empty:
        return {"error": "No hay columnas numéricas"}

    resultado = {}

    for col in num_df.columns:
        serie = num_df[col].dropna()
        q1  = float(serie.quantile(0.25))
        q3  = float(serie.quantile(0.75))
        iqr = q3 - q1
        lim_inf = q1 - 1.5 * iqr
        lim_sup = q3 + 1.5 * iqr

        outliers_mask = (serie < lim_inf) | (serie > lim_sup)
        outliers_vals = serie[outliers_mask]

        resultado[col] = {
            "q1":             round(q1, 4),
            "q3":             round(q3, 4),
            "iqr":            round(iqr, 4),
            "limite_inf":     round(lim_inf, 4),
            "limite_sup":     round(lim_sup, 4),
            "total_outliers": int(outliers_mask.sum()),
            "pct_outliers":   round(outliers_mask.sum() / len(serie) * 100, 2),
            "min_outlier":    round(float(outliers_vals.min()), 4) if len(outliers_vals) else None,
            "max_outlier":    round(float(outliers_vals.max()), 4) if len(outliers_vals) else None,
        }

    return {"columnas": resultado}


# =========================
# POST /clean
# Endpoint genérico — recibe
# cualquier CSV, aplica el
# pipeline de cleaning.py y
# devuelve el CSV limpio como
# archivo descargable + stats
# =========================
from fastapi.responses import StreamingResponse

@app.post("/clean")
async def clean_generic(file: UploadFile = File(...)):

    contents = await file.read()
    df = pd.read_csv(io.BytesIO(contents))

    filas_antes  = len(df)
    cols_antes   = list(df.columns)
    nulos_antes  = int(df.isnull().sum().sum())
    dupes_antes  = int(df.duplicated().sum())
    tipos_antes  = {col: str(dtype) for col, dtype in df.dtypes.items()}

    # ── Pipeline usando funciones de cleaning.py ──────────
    pasos = []

    df = normalize_column_names(df)
    pasos.append("normalize_column_names ✓")

    df = strip_strings(df)
    pasos.append("strip_strings ✓")

    df = drop_empty_rows(df)
    pasos.append("drop_empty_rows ✓")

    df = remove_duplicates(df)
    pasos.append("remove_duplicates ✓")

    if "email" in df.columns:
        df = remove_invalid_emails(df)
        pasos.append("remove_invalid_emails ✓")

    if "first_name" in df.columns and "last_name" in df.columns:
        df = create_full_name(df)
        pasos.append("create_full_name ✓")

    if "age" in df.columns:
        df = create_is_adult(df)
        pasos.append("create_is_adult ✓")

    filas_despues = len(df)
    cols_despues  = list(df.columns)
    tipos_despues = {col: str(dtype) for col, dtype in df.dtypes.items()}

    # ── Estadísticas finales ──────────────────────────────
    desc_raw    = df.describe(include="all").fillna("").to_dict()
    descripcion = {
        col: {k: str(v) for k, v in stats.items()}
        for col, stats in desc_raw.items()
    }
    nulos_despues = {k: int(v) for k, v in df.isnull().sum().items()}
    preview       = df.head(100).fillna("").to_dict(orient="records")

    # ── CSV limpio como string base64 ─────────────────────
    import base64
    csv_bytes  = df.to_csv(index=False).encode("utf-8")
    csv_b64    = base64.b64encode(csv_bytes).decode("utf-8")
    nombre_out = file.filename.replace(".csv", "_clean.csv")

    return {
        "mensaje":         "Pipeline ejecutado correctamente",
        "archivo_entrada": file.filename,
        "archivo_salida":  nombre_out,
        "pasos":           pasos,
        "antes": {
            "filas":    filas_antes,
            "columnas": cols_antes,
            "nulos":    nulos_antes,
            "dupes":    dupes_antes,
            "tipos":    tipos_antes,
        },
        "despues": {
            "filas":       filas_despues,
            "columnas":    cols_despues,
            "nulos":       nulos_despues,
            "tipos":       tipos_despues,
            "descripcion": descripcion,
        },
        "preview":  preview,
        "csv_b64":  csv_b64,
    }


# =========================
# POST /limpiar
# Recibe cualquier CSV desde
# el dashboard, aplica el
# pipeline y devuelve el
# resultado limpio en JSON
# =========================
@app.post("/limpiar")
async def limpiar_csv(file: UploadFile = File(...)):

    contents = await file.read()
    df = pd.read_csv(io.BytesIO(contents))

    filas_antes = len(df)
    nulos_antes = int(df.isnull().sum().sum())
    dupes_antes = int(df.duplicated().sum())

    # Pipeline completo (igual que /procesar)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w]", "",  regex=True)
    )
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    df = df.dropna()
    df = df.drop_duplicates()

    filas_despues = len(df)
    preview       = df.head(100).fillna("").to_dict(orient="records")

    return {
        "mensaje":            "Limpieza completada",
        "filas_antes":        filas_antes,
        "filas_despues":      filas_despues,
        "filas_eliminadas":   filas_antes - filas_despues,
        "nulos_antes":        nulos_antes,
        "duplicados_antes":   dupes_antes,
        "columnas_resultado": list(df.columns),
        "preview":            preview,
    }


# =========================
# POST /datos
# Devuelve valores crudos de
# columnas numéricas y top
# valores de columnas categ.
# para gráficas interactivas
# Muestra hasta 5000 filas
# para no saturar el browser
# =========================
@app.post("/datos")
async def datos_csv(file: UploadFile = File(...)):

    contents = await file.read()
    df = _limpiar_df(pd.read_csv(io.BytesIO(contents)))

    # Muestrear si el dataset es muy grande
    MAX_ROWS = 5000
    if len(df) > MAX_ROWS:
        df_sample = df.sample(MAX_ROWS, random_state=42)
    else:
        df_sample = df

    num_cols = df.select_dtypes(include=["int64","float64"]).columns.tolist()
    cat_cols = df.select_dtypes(include=["object"]).columns.tolist()

    # Valores por columna numérica (para boxplot, violin, histograma)
    numericos = {}
    for col in num_cols:
        vals = df_sample[col].dropna().tolist()
        numericos[col] = vals

    # Top 15 valores más frecuentes por columna categórica
    categoricos = {}
    for col in cat_cols:
        vc = df[col].value_counts().head(15)
        categoricos[col] = {
            "labels": vc.index.tolist(),
            "values": vc.values.tolist(),
        }

    return {
        "filas_totales":  len(df),
        "filas_muestra":  len(df_sample),
        "muestreado":     len(df) > MAX_ROWS,
        "cols_numericas": num_cols,
        "cols_categoricas": cat_cols,
        "numericos":      numericos,
        "categoricos":    categoricos,
    }