# ✈️ Flight Delay Predictor: End-to-End ELT Pipeline

## 📌 Descripción del Proyecto
Este proyecto es una arquitectura de datos Batch completa (ELT) diseñada para procesar históricamente casi 6 millones de registros de vuelos. El objetivo es ingerir datos crudos, transformarlos masivamente y entrenar un modelo de Machine Learning capaz de predecir retrasos aéreos, resolviendo problemas reales como la escalabilidad de memoria y el desbalanceo de clases.


## 🏗️ Arquitectura del Pipeline

![Diagrama de Arquitectura ELT](images/arquitectura_elt.png)

## 🛠️ Arquitectura y Tecnologías
* **Orquestación:** Apache Airflow (Gestión de dependencias y automatización).
* **Infraestructura:** Docker & Docker Compose (Contenedorización de servicios y resolución de redes).
* **Almacenamiento (Data Warehouse):** PostgreSQL.
* **Transformación (Modern Data Stack):** dbt (Data Build Tool) para limpieza de datos mediante SQL puro.
* **Machine Learning:** Python (Scikit-Learn, Pandas) en entornos virtuales aislados.

## 🚀 Flujo de Trabajo (Pipeline)
1. **Extracción y Carga (EL):** Un DAG en Airflow valida la existencia del dataset masivo y utiliza el comando `COPY` de PostgreSQL para ingerir 5.8 millones de registros en segundos hacia la capa *Raw*.
2. **Transformación (T):** Mediante `dbt`, se procesa la capa *Raw* para crear una capa *Staging* (limpieza de nulos y casteos) y finalmente un *Data Mart* de características (Feature Engineering), filtrando ruido y creando variables objetivo.
3. **Consumo (ML):** Un script de Python extrae una muestra representativa del Data Mart y entrena un modelo `RandomForestClassifier`. Se aplicó balanceo de clases (`class_weight='balanced'`) para priorizar el *recall* en la detección efectiva de vuelos retrasados.

## 📂 Estructura del Proyecto

```text
flight_delay_pipeline/
├── dags/
│   └── 01_flight_pipeline.py        # DAG de Airflow para la ingesta
├── data/
│   └── flights_raw.csv              # Dataset masivo (Ignorado en git)
├── transformacion_vuelos/           # Proyecto dbt
│   ├── models/
│   │   ├── stg_vuelos.sql           # Capa Staging (Limpieza)
│   │   └── mart_features_vuelos.sql # Capa Data Mart (Features ML)
│   └── dbt_project.yml              # Configuración de dbt
├── entrenar_modelo.py               # Script de Machine Learning
├── docker-compose.yaml              # Infraestructura (Airflow + Postgres)
└── README.md

## Cómo ejecutar este proyecto
1. **Requisitos previos**
Docker Desktop instalado y corriendo.
Python 3.11 o superior.
Archivo de datos flights_raw.csv ubicado en la carpeta data/.

2. **Levantar la infraestructura**
docker compose up -d
Acceder a Airflow en http://localhost:8080 y ejecutar el DAG 01_prediccion_retrasos_vuelos para ingerir los datos.

3. **Transformación con dbt**
En un entorno virtual de Python, instala el adaptador y ejecuta los modelos:
pip install dbt-postgres
cd transformacion_vuelos
dbt run

4. **Entrenar el modelo de Machine Learning**
Regresa a la carpeta raíz y ejecuta el script predictivo:
pip install pandas scikit-learn sqlalchemy psycopg2-binary
python entrenar_modelo.py


## 📊 Resultados del Modelo (Manejo de Desbalanceo)
Al conectar el modelo a los datos limpios, se observó que la exactitud (Accuracy) inicial escondía un modelo "perezoso" que no detectaba los retrasos reales.
Aplicando class_weight='balanced' en el Random Forest, el modelo logró pasar de un recall de **0.0 a 0.56** en la detección de la clase minoritaria (vuelos retrasados), demostrando capacidad analítica para resolver la paradoja de la exactitud.