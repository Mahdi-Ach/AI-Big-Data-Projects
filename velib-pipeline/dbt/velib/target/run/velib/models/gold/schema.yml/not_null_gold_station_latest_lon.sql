
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lon
from "velib"."gold"."gold_station_latest"
where lon is null



  
  
      
    ) dbt_internal_test