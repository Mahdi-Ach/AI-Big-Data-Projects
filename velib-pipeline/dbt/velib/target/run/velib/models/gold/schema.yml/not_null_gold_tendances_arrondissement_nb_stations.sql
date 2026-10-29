
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nb_stations
from "velib"."gold"."gold_tendances_arrondissement"
where nb_stations is null



  
  
      
    ) dbt_internal_test