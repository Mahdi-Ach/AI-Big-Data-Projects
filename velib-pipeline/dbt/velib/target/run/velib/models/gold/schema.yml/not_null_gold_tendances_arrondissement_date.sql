
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from "velib"."gold"."gold_tendances_arrondissement"
where date is null



  
  
      
    ) dbt_internal_test