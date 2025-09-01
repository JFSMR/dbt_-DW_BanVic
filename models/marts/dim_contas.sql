with
     -- importando tabelas
     contas as (
         select *
         from  {{ ref('int_contas') }}
     )
     select *
     from contas 