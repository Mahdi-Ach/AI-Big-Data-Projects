
  
    

  create  table "velib"."gold"."gold_stations_tension__dbt_tmp"
  
  
    as
  
  (
    

WITH base AS (
    SELECT
        station_id,
        name,
        date,
        num_bikes_available,
        capacity
    FROM "velib"."public"."silver_station_status_enriched"
    WHERE capacity > 0
      AND num_bikes_available IS NOT NULL
)

SELECT
    station_id,
    name,
    COUNT(*)                                          AS nb_mesures,
    SUM(CASE WHEN num_bikes_available = 0 THEN 1 ELSE 0 END) AS nb_fois_vide,
    SUM(CASE WHEN num_bikes_available = capacity THEN 1 ELSE 0 END) AS nb_fois_pleine,
    ROUND(100.0 * SUM(CASE WHEN num_bikes_available = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_vide,
    ROUND(100.0 * SUM(CASE WHEN num_bikes_available = capacity THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_pleine
FROM base
GROUP BY station_id, name
HAVING COUNT(*) >= 10
ORDER BY pct_vide DESC, pct_pleine DESC
  );
  