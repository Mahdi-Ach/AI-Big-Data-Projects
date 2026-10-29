
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select zone_approx
from "velib"."gold"."gold_tendances_arrondissement"
where zone_approx is null



  
  
      
    ) dbt_internal_test