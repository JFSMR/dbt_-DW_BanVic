with
     -- importando tabelas
     colaboradores as (
         select *
         from  {{ ref('stg_erp__colaboradores') }}
     )
     select *
     from colaboradores 