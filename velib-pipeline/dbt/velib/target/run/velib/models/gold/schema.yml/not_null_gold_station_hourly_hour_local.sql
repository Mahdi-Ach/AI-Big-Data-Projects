
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hour_local
from "velib"."gold"."gold_station_hourly"
where hour_local is null



  
  
      
    ) dbt_internal_test