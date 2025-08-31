with
     -- importando tabelas
     colaborador_age as (
         select *
         from  {{ ref('stg_erp__colaborador_agencias') }}
     )
     select *
     from  colaborador_age