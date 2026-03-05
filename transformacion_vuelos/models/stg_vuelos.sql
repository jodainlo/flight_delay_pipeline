{{ config(materialized='table') }}

SELECT
    -- Fechas
    CAST(year AS INTEGER) AS anio,
    CAST(month AS INTEGER) AS mes,
    CAST(day AS INTEGER) AS dia,
    
    -- Identificadores
    airline AS aerolinea,
    flight_number AS numero_vuelo,
    origin_airport AS origen,
    destination_airport AS destino,
    
    -- Retrasos (Convertidos a decimales. Si viene vacío '', lo hace NULL)
    CAST(NULLIF(departure_delay, '') AS FLOAT) AS retraso_salida_minutos,
    CAST(NULLIF(arrival_delay, '') AS FLOAT) AS retraso_llegada_minutos,
    
    -- Otras métricas
    CAST(NULLIF(distance, '') AS FLOAT) AS distancia_millas,
    CAST(NULLIF(cancelled, '') AS INTEGER) AS cancelado

FROM vuelos_historicos_raw