
  
    

  create  table "velib"."gold"."gold_tendances_arrondissement__dbt_tmp"
  
  
    as
  
  (
    

WITH base AS (
    SELECT
        station_id,
        name,
        lat,
        lon,
        date,
        num_bikes_available,
        capacity,
        taux_remplissage
    FROM "velib"."public"."silver_station_status_enriched"
    WHERE capacity > 0
      AND num_bikes_available IS NOT NULL
      AND lat IS NOT NULL
      AND lon IS NOT NULL
)

SELECT
    -- Approximation : arrondissement en fonction des coordonnées
    CASE
        WHEN lat BETWEEN 48.85 AND 48.87 AND lon BETWEEN 2.30 AND 2.35 THEN '1-4e'
        WHEN lat BETWEEN 48.85 AND 48.87 AND lon BETWEEN 2.35 AND 2.40 THEN '5-6e'
        WHEN lat BETWEEN 48.85 AND 48.88 AND lon BETWEEN 2.25 AND 2.30 THEN '7-8e'
        WHEN lat BETWEEN 48.87 AND 48.89 AND lon BETWEEN 2.25 AND 2.40 THEN '9-10e'
        WHEN lat BETWEEN 48.84 AND 48.87 AND lon BETWEEN 2.35 AND 2.42 THEN '11-12e'
        WHEN lat BETWEEN 48.82 AND 48.85 AND lon BETWEEN 2.30 AND 2.40 THEN '13-14e'
        WHEN lat BETWEEN 48.83 AND 48.86 AND lon BETWEEN 2.25 AND 2.32 THEN '15-16e'
        WHEN lat BETWEEN 48.87 AND 48.90 AND lon BETWEEN 2.28 AND 2.36 THEN '17-18e'
        WHEN lat BETWEEN 48.85 AND 48.89 AND lon BETWEEN 2.36 AND 2.42 THEN '19-20e'
        ELSE 'Hors zone'
    END AS zone_approx,
    date,
    COUNT(DISTINCT station_id)         AS nb_stations,
    COUNT(*)                            AS nb_mesures,
    AVG(taux_remplissage)::numeric(5,3) AS avg_taux_remplissage,
    AVG(num_bikes_available)::numeric(10,2) AS avg_bikes
FROM base
GROUP BY zone_approx, date
ORDER BY date, zone_approx
  );
  