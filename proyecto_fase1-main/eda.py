import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ======================================
# 1. Configuración
# ======================================
sns.set(style="whitegrid")

# ======================================
# 2. Cargar dataset
# ======================================
df = pd.read_csv("data/customers_80k_dirty.csv")

print("\n==============================")
print("INFORMACIÓN GENERAL DEL DATASET")
print("==============================")

print("\nPrimeras filas:")
print(df.head())

print("\nInformación del dataset:")
print(df.info())

print("\nDimensiones del dataset:")
print(df.shape)

print("\nValores nulos:")
print(df.isnull().sum())

print("\nDuplicados:")
print(df.duplicated().sum())

# ======================================
# 3. Estadísticas descriptivas
# ======================================
print("\n==============================")
print("ESTADÍSTICAS DESCRIPTIVAS")
print("==============================")

print(df.describe())

# ======================================
# 4. Crear variables nuevas
# ======================================

# nombre completo
df["full_name"] = df["first_name"] + " " + df["last_name"]

# adulto
df["is_adult"] = df["age"].apply(
    lambda x: True if x >= 18 else False if pd.notnull(x) else None
)

# dominio del email
df["email_domain"] = df["email"].str.split("@").str[1]

# longitud del nombre
df["name_length"] = df["full_name"].str.len()

# convertir fecha
df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")

# ======================================
# 5. Distribución de edades
# ======================================
plt.figure()
plt.hist(df["age"].dropna(), bins=20)
plt.title("Distribución de edades")
plt.xlabel("Edad")
plt.ylabel("Frecuencia")
plt.show()

# ======================================
# 6. Boxplot de edades
# ======================================
plt.figure()
sns.boxplot(x=df["age"])
plt.title("Boxplot de edades")
plt.show()

# ======================================
# 7. Adultos vs menores
# ======================================
plt.figure()
df["is_adult"].value_counts().plot(kind="bar")
plt.title("Adultos vs menores")
plt.xlabel("Es adulto")
plt.ylabel("Cantidad")
plt.show()

# ======================================
# 8. Distribución por género
# ======================================
plt.figure()
df["gender"].value_counts().plot(kind="bar")
plt.title("Distribución por género")
plt.xlabel("Género")
plt.ylabel("Cantidad")
plt.show()

# ======================================
# 9. Top ciudades
# ======================================
plt.figure()
df["city"].value_counts().head(10).plot(kind="bar")
plt.title("Top 10 ciudades")
plt.xlabel("Ciudad")
plt.ylabel("Cantidad")
plt.show()

# ======================================
# 10. Top países
# ======================================
plt.figure()
df["country"].value_counts().head(10).plot(kind="bar")
plt.title("Top 10 países")
plt.xlabel("País")
plt.ylabel("Cantidad")
plt.show()

# ======================================
# 11. Dominios de email
# ======================================
print("\nDominios de email más comunes:")
print(df["email_domain"].value_counts().head(10))

plt.figure()
df["email_domain"].value_counts().head(10).plot(kind="bar")
plt.title("Top dominios de email")
plt.xlabel("Dominio")
plt.ylabel("Cantidad")
plt.show()

# ======================================
# 12. Distribución de longitud de nombres
# ======================================
plt.figure()
sns.histplot(df["name_length"], bins=20, kde=True)
plt.title("Distribución longitud de nombres")
plt.show()

# ======================================
# 13. Scatter edad vs longitud de nombre
# ======================================
plt.figure()
sns.scatterplot(x=df["age"], y=df["name_length"])
plt.title("Edad vs longitud de nombre")
plt.show()

# ======================================
# 14. Registros por año de registro
# ======================================
df["signup_year"] = df["signup_date"].dt.year

plt.figure()
df["signup_year"].value_counts().sort_index().plot(kind="line")
plt.title("Clientes registrados por año")
plt.xlabel("Año")
plt.ylabel("Cantidad")
plt.show()

# ======================================
# 15. Heatmap de correlaciones
# ======================================
corr = df.corr(numeric_only=True)

plt.figure()
sns.heatmap(corr, annot=True)
plt.title("Matriz de correlación")
plt.show()

# ======================================
# 16. Distribución acumulada edad
# ======================================
plt.figure()
sns.ecdfplot(df["age"].dropna())
plt.title("Distribución acumulada de edad")
plt.show()

# ======================================
# 17. Estadísticas finales
# ======================================
print("\n==============================")
print("CONCLUSIONES DEL ANÁLISIS")
print("==============================")

print(f"Total de registros: {len(df)}")
print(f"Edad promedio: {df['age'].mean():.2f}")
print(f"Edad mínima: {df['age'].min()}")
print(f"Edad máxima: {df['age'].max()}")

print("\nTop 5 ciudades:")
print(df["city"].value_counts().head())

print("\nTop 5 países:")
print(df["country"].value_counts().head())

print("\nTop 5 dominios de email:")
print(df["email_domain"].value_counts().head())

print("\nAnálisis exploratorio finalizado.")