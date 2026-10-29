
    
    

select
    station_id as unique_field,
    count(*) as n_records

from "velib"."gold"."gold_stations_tension"
where station_id is not null
group by station_id
having count(*) > 1


