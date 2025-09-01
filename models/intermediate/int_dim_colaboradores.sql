with
     -- importando tabelas
     colaboradores as (
         select  
           id_colaborador
             , nome_completo
          
         from  {{ ref('stg_erp__colaboradores') }}
)
,
 agencia_colaborador as (
  select 
     id_agencia
     ,id_colaborador
     ,row_number() over (partition by id_colaborador
     order by id_agencia ) as rn
    from  {{ ref('stg_erp__colaborador_agencias') }}

 )

     select 
      c.id_colaborador
         ,ca.id_agencia
        ,c.nome_completo
      
       

     from colaboradores  as c
     inner join agencia_colaborador as ca
      on c.id_colaborador = ca.id_colaborador
      where ca.rn = 1