with
     -- importando tabelas
     clientes as (
         select *
         from  {{ ref('Int_clientes') }}
     )
     select *
     from clientes