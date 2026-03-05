from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

# Tarea 1: Verificar archivo
def verificar_archivo_csv():
    ruta_archivo = '/opt/airflow/data/flights_raw.csv'
    if os.path.exists(ruta_archivo):
        tamano_mb = os.path.getsize(ruta_archivo) / (1024 * 1024)
        print(f"¡Exito! Archivo encontrado correctamente.")
        print(f"Tamaño del dataset: {tamano_mb:.2f} MB")
        return "Archivo validado"
    else:
        raise FileNotFoundError(f"Error: No se encontro el archivo en {ruta_archivo}")

# Tarea 2: Crear la tabla e inyectar los datos masivos
def cargar_datos_postgres():
    # Nos conectamos usando el puente que creamos en la interfaz
    hook = PostgresHook(postgres_conn_id='conexion_postgres_vuelos')
    
    # 1. Creamos la tabla receptora (Todo temporalmente como texto para no perder datos en la ingesta bruta)
    crear_tabla_sql = """
    CREATE TABLE IF NOT EXISTS vuelos_historicos_raw (
        YEAR VARCHAR(255), MONTH VARCHAR(255), DAY VARCHAR(255), DAY_OF_WEEK VARCHAR(255),
        AIRLINE VARCHAR(255), FLIGHT_NUMBER VARCHAR(255), TAIL_NUMBER VARCHAR(255),
        ORIGIN_AIRPORT VARCHAR(255), DESTINATION_AIRPORT VARCHAR(255), SCHEDULED_DEPARTURE VARCHAR(255),
        DEPARTURE_TIME VARCHAR(255), DEPARTURE_DELAY VARCHAR(255), TAXI_OUT VARCHAR(255),
        WHEELS_OFF VARCHAR(255), SCHEDULED_TIME VARCHAR(255), ELAPSED_TIME VARCHAR(255),
        AIR_TIME VARCHAR(255), DISTANCE VARCHAR(255), WHEELS_ON VARCHAR(255), TAXI_IN VARCHAR(255),
        SCHEDULED_ARRIVAL VARCHAR(255), ARRIVAL_TIME VARCHAR(255), ARRIVAL_DELAY VARCHAR(255),
        DIVERTED VARCHAR(255), CANCELLED VARCHAR(255), CANCELLATION_REASON VARCHAR(255),
        AIR_SYSTEM_DELAY VARCHAR(255), SECURITY_DELAY VARCHAR(255), AIRLINE_DELAY VARCHAR(255),
        LATE_AIRCRAFT_DELAY VARCHAR(255), WEATHER_DELAY VARCHAR(255)
    );
    
    -- Limpiamos la tabla por si corremos el DAG varias veces
    TRUNCATE TABLE vuelos_historicos_raw;
    """
    hook.run(crear_tabla_sql)
    print("Tabla creada y limpia.")

    # 2. Inyectamos los datos a la velocidad del rayo usando COPY
    print("Iniciando la ingesta masiva de datos. Esto puede tomar unos segundos...")
    hook.copy_expert(
        sql="COPY vuelos_historicos_raw FROM STDIN WITH CSV HEADER",
        filename='/opt/airflow/data/flights_raw.csv'
    )
    print("¡Carga masiva completada con exito!")

# Configuración del DAG
default_args = {
    'owner': 'jorge_data_engineer',
    'retries': 1,
}

with DAG(
    dag_id='01_prediccion_retrasos_vuelos',
    default_args=default_args,
    description='Pipeline Batch para predecir retrasos de vuelos',
    start_date=datetime(2026, 3, 1),
    schedule_interval='@once',
    catchup=False,
    tags=['batch', 'machine_learning'],
) as dag:

    tarea_inicio = BashOperator(
        task_id='iniciar_pipeline',
        bash_command='echo "Iniciando el pipeline de vuelos historicos..."'
    )

    tarea_verificar_datos = PythonOperator(
        task_id='verificar_dataset_bruto',
        python_callable=verificar_archivo_csv
    )

    tarea_cargar_postgres = PythonOperator(
        task_id='ingesta_masiva_postgres',
        python_callable=cargar_datos_postgres
    )

    # El nuevo flujo de 3 pasos
    tarea_inicio >> tarea_verificar_datos >> tarea_cargar_postgres