-- Disponibilité moyenne par station et par heure (heure de Paris)
with status as (
    select
        station_id,
        num_bikes_available,
        num_docks_available,
        (reported_at at time zone 'UTC') at time zone 'Europe/Paris' as reported_local
    from "velib"."silver"."station_status"
    where is_installed
)

select
    s.station_id,
    i.name,
    i.capacity,
    date_trunc('hour', s.reported_local)                        as hour_local,
    count(*)                                                    as nb_reports,
    round(avg(s.num_bikes_available), 2)                        as avg_bikes,
    round(avg(s.num_docks_available), 2)                        as avg_docks,
    min(s.num_bikes_available)                                  as min_bikes,
    max(s.num_bikes_available)                                  as max_bikes,
    round(avg(s.num_bikes_available::numeric / nullif(i.capacity, 0)), 3) as avg_fill_rate
from status s
join "velib"."silver"."station_information" i using (station_id)
group by 1, 2, 3, 4