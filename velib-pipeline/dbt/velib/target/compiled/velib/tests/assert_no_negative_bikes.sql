-- Test singulier : ne doit retourner aucune ligne
select * from "velib"."gold"."gold_station_latest"
where num_bikes_available < 0 or num_docks_available < 0