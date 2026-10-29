
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date
from "velib"."gold"."gold_remplissage_horaire"
where date is null



  
  
      
    ) dbt_internal_test