
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_taux_remplissage
from "velib"."gold"."gold_remplissage_horaire"
where avg_taux_remplissage is null



  
  
      
    ) dbt_internal_test