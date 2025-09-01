with
 fct_propostas_transacoes as (
   select *
   from {{ ref('int_fct_propostas_transacoes') }} 
 )

 select * from  fct_propostas_transacoes