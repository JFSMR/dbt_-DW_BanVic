with
     -- importando tabelas
     agencias as (
         select *
         from  {{ ref('stg_erp__agencias') }}
     )
     select *
     from agencias 