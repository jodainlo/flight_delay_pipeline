{{ config(materialized='table') }}

SELECT
    -- Características (Features)
    anio,
    mes,
    dia,
    aerolinea,
    origen,
    destino,
    distancia_millas,
    
    -- Variable Objetivo (Target) para el algoritmo:
    -- Consideramos "retraso" si el vuelo salió más de 15 minutos tarde
    CASE 
        WHEN retraso_salida_minutos > 15 THEN 1 
        ELSE 0 
    END AS es_vuelo_retrasado

-- Usamos ref() para leer de nuestra capa limpia en lugar de la tabla cruda
FROM {{ ref('stg_vuelos') }}

-- Filtramos el ruido: no nos sirven los vuelos cancelados ni los que no tienen datos de salida
WHERE cancelado = 0 
  AND retraso_salida_minutos IS NOT NULL