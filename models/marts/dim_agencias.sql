with
     -- importando tabelas
     agencias as (
         select *
         from  {{ ref('int_agencias') }}
     )
     select *
     from agencias 