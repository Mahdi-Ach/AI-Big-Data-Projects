-- Dernier état connu de chaque station (pour la carte du dashboard)
with ranked as (
    select
        s.*,
        row_number() over (partition by s.station_id order by s.reported_at desc) as rn
    from "velib"."silver"."station_status" s
)

select
    r.station_id,
    i.name,
    i.lat,
    i.lon,
    i.capacity,
    r.num_bikes_available,
    r.bikes_mechanical,
    r.bikes_ebike,
    r.num_docks_available,
    r.is_renting,
    r.is_returning,
    (r.reported_at at time zone 'UTC') at time zone 'Europe/Paris' as reported_local
from ranked r
join "velib"."silver"."station_information" i using (station_id)
where r.rn = 1