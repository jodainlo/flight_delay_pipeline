import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score

print("Conectando a la base de datos PostgreSQL...")
# Conexión directa al puerto 5434 que configuramos en Docker
cadena_conexion = 'postgresql://airflow:airflow@localhost:5434/airflow'
motor = create_engine(cadena_conexion)

print("Extrayendo una muestra de 100,000 vuelos...")
# Usamos una consulta SQL para traer solo las variables clave y evitar el colapso de RAM
consulta = """
SELECT mes, dia, aerolinea, distancia_millas, es_vuelo_retrasado
FROM mart_features_vuelos
ORDER BY RANDOM()
LIMIT 100000;
"""
df = pd.read_sql(consulta, motor)

print("Preprocesando los datos...")
# Las aerolíneas son texto, el modelo necesita números. 
# Usamos One-Hot Encoding para convertirlas en columnas binarias (0 y 1)
df_preparado = pd.get_dummies(df, columns=['aerolinea'], drop_first=True)

# Separar las características (X) y nuestra variable objetivo (y)
X = df_preparado.drop('es_vuelo_retrasado', axis=1)
y = df_preparado['es_vuelo_retrasado']

# Dividir los datos: 80% para entrenar, 20% para el examen final (prueba)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print("Entrenando el modelo de Bosque Aleatorio (Random Forest)...")
# Usamos parámetros ligeros para que el entrenamiento sea rápido
modelo = RandomForestClassifier(n_estimators=50, max_depth=10, random_state=42, n_jobs=-1, class_weight='balanced')
modelo.fit(X_train, y_train)

print("Evaluando las predicciones...")
predicciones = modelo.predict(X_test)
exactitud = accuracy_score(y_test, predicciones)

print("\n" + "="*50)
print(f"Exactitud del modelo (Accuracy): {exactitud:.2%}")
print("="*50)
print("\nReporte detallado de clasificación:")
print(classification_report(y_test, predicciones))