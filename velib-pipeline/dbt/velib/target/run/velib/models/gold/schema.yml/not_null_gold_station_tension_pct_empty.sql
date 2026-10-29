
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pct_empty
from "velib"."gold"."gold_station_tension"
where pct_empty is null



  
  
      
    ) dbt_internal_test