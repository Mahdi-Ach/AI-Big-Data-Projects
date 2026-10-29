
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select heure
from "velib"."gold"."gold_remplissage_horaire"
where heure is null



  
  
      
    ) dbt_internal_test