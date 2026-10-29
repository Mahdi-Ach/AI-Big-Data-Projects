
  
    

  create  table "velib"."gold"."gold_station_tension__dbt_tmp"
  
  
    as
  
  (
    -- Stations souvent vides ou souvent pleines (part des relevés concernés)
select
    s.station_id,
    i.name,
    i.lat,
    i.lon,
    i.capacity,
    count(*)                                                          as nb_reports,
    round(avg((s.num_bikes_available = 0)::int)::numeric, 3)          as pct_empty,
    round(avg((s.num_docks_available = 0)::int)::numeric, 3)          as pct_full
from "velib"."silver"."station_status" s
join "velib"."silver"."station_information" i using (station_id)
where s.is_installed
group by 1, 2, 3, 4, 5
  );
  