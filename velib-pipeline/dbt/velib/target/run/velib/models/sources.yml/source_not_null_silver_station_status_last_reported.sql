
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select last_reported
from "velib"."silver"."station_status"
where last_reported is null



  
  
      
    ) dbt_internal_test