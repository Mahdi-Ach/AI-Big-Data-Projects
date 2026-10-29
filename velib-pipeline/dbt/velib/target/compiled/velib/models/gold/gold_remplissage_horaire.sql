

WITH base AS (
    SELECT
        station_id,
        name,
        date,
        EXTRACT(HOUR FROM (ingested_at::timestamptz)) AS heure,
        num_bikes_available,
        capacity,
        taux_remplissage
    FROM "velib"."public"."silver_station_status_enriched"
    WHERE capacity > 0
      AND num_bikes_available IS NOT NULL
)

SELECT
    station_id,
    name,
    date,
    heure,
    COUNT(*)                            AS nb_mesures,
    AVG(num_bikes_available)::numeric(10,2) AS avg_bikes,
    AVG(capacity)::numeric(10,2)             AS avg_capacity,
    AVG(taux_remplissage)::numeric(5,3)      AS avg_taux_remplissage
FROM base
GROUP BY station_id, name, date, heure