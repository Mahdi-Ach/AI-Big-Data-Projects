
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Test singulier : ne doit retourner aucune ligne
select * from "velib"."gold"."gold_station_latest"
where num_bikes_available < 0 or num_docks_available < 0
  
  
      
    ) dbt_internal_test