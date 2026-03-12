from typing import Callable
from functools import wraps
import pandas as pd


# =========================
# Decorador para loggear pasos
# =========================
def log_step(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Running: {func.__name__}")
        result = func(*args, **kwargs)
        print(f"Done: {func.__name__}")
        return result
    return wrapper


# =========================
# Normalizar nombres de columnas
# =========================
@log_step
def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    return df


# =========================
# Eliminar duplicados
# =========================
@log_step
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:

    if "customer_id" in df.columns:
        return df.drop_duplicates(subset="customer_id")

    return df.drop_duplicates()


# =========================
# Eliminar filas completamente vacías
# =========================
@log_step
def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(how="all")


# =========================
# Limpiar espacios en strings
# =========================
@log_step
def strip_strings(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: x.strip() if isinstance(x, str) else x
            )

    return df


# =========================
# Eliminar emails inválidos
# =========================
@log_step
def remove_invalid_emails(df: pd.DataFrame) -> pd.DataFrame:

    if "email" not in df.columns:
        return df

    return df[df["email"].str.contains("@", na=False)]


# =========================
# Crear columna full_name
# =========================
@log_step
def create_full_name(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    if "first_name" in df.columns and "last_name" in df.columns:
        df["full_name"] = df["first_name"] + " " + df["last_name"]

    return df


# =========================
# Crear columna is_adult
# =========================
@log_step
def create_is_adult(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    if "age" in df.columns:
        df["is_adult"] = df["age"] >= 18

    return df