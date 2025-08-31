with
     -- importando tabelas
     contas as (
         select *
         from  {{ ref('stg_erp__contas') }}
     )
     select *
     from contas 