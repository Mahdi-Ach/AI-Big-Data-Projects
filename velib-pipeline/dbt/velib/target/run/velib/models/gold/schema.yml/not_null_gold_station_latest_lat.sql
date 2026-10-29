
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lat
from "velib"."gold"."gold_station_latest"
where lat is null



  
  
      
    ) dbt_internal_test