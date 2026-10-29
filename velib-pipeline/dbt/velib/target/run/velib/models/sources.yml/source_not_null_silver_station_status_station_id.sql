
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select station_id
from "velib"."silver"."station_status"
where station_id is null



  
  
      
    ) dbt_internal_test