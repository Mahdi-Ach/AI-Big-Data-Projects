
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nb_mesures
from "velib"."gold"."gold_stations_tension"
where nb_mesures is null



  
  
      
    ) dbt_internal_test