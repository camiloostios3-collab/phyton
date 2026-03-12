from pathlib import Path
import pandas as pd
from pydantic import ValidationError

from file_io import read_csv, write_csv

from cleaning import (
    normalize_column_names,
    remove_duplicates,
    drop_empty_rows,
    strip_strings,
    remove_invalid_emails,
    create_full_name,
    create_is_adult
)

from schemas import InputPersonaSchema, OutputPersonaSchema


def main() -> None:

    # =========================
    # 1️⃣ Rutas del proyecto
    # =========================
    base_path = Path(__file__).resolve().parent

    data_path = base_path / "data"
    input_path = data_path / "customers_80k_dirty.csv"
    output_path = data_path / "customers_clean.csv"

    # =========================
    # 2️⃣ Leer dataset
    # =========================
    df = read_csv(input_path)

    print("=== Estadísticas iniciales ===")
    print(f"Filas: {df.shape[0]}")
    print(f"Columnas: {df.shape[1]}")
    print(f"Duplicados: {df.duplicated().sum()}")
    print(f"Valores nulos:\n{df.isnull().sum()}\n")

    # =========================
    # 3️⃣ Normalizar columnas
    # =========================
    df = normalize_column_names(df)

    # =========================
    # 4️⃣ Limpieza
    # =========================
    df = strip_strings(df)
    df = remove_duplicates(df)
    df = drop_empty_rows(df)
    df = remove_invalid_emails(df)

    # =========================
    # 5️⃣ Transformaciones
    # =========================
    df = create_full_name(df)
    df = create_is_adult(df)

    # =========================
    # 6️⃣ Validación de datos
    # =========================
    clean_records = []
    errores = 0

    for i, record in enumerate(df.to_dict(orient="records")):

        try:
            # Validación de entrada
            input_data = InputPersonaSchema(**record)

            # Construcción del registro de salida
            output_data = OutputPersonaSchema(
                customer_id=input_data.customer_id,
                full_name=f"{input_data.first_name} {input_data.last_name}",
                email=input_data.email,
                age=input_data.age
            )

            clean_records.append(output_data.model_dump())

        except ValidationError as e:
            print(f"\n❌ Error en fila {i}: {e}")
            errores += 1
            continue

    print("\n✅ Validación terminada")

    # =========================
    # 7️⃣ Crear DataFrame limpio
    # =========================
    df_clean = pd.DataFrame(clean_records)

    # =========================
    # 8️⃣ Guardar CSV limpio
    # =========================
    write_csv(df_clean, output_path)

    # =========================
    # 9️⃣ Estadísticas finales
    # =========================
    print("\n=== Estadísticas finales ===")
    print(f"Filas limpias: {df_clean.shape[0]}")
    print(f"Errores encontrados: {errores}")

    print("\nPrimeras 5 filas:")
    print(df_clean.head())

    print("\n🎉 Proceso finalizado correctamente.")


if __name__ == "__main__":
    main()