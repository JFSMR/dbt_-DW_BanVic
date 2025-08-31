with
-- importando dados para enriquecer a tabela 
 
 transacoes as (
 select 
  id_transacao
    ,id_num_conta
    , data_transacao
    ,nome_transacao
    ,valor_transacao
  from {{ ref('stg_erp__transacoes') }} 


)
,
 contas as (
      select 
         id_num_conta
         ,id_cliente      
         ,id_agencia
         ,id_colaborador
         ,tipo_conta
         ,data_abertura
         ,saldo_total
         ,saldo_disponivel
         ,data_ultimo_lancamento
 
     from {{ ref('stg_erp__contas') }}


 ), clientes as (
      select 
        id_cliente
         ,nome_completo
         ,email 
         ,tipo_cliente 
         ,data_inclusao
         ,cpfcnpj
         ,data_nascimento
         ,endereco
         ,cep
      from {{ ref('stg_erp__clientes') }}
 ),
   agencias as (
      select 
         id_agencia
          ,nome_agencia
          ,endereco 
          ,cidade
          ,estado
          ,data_abertura
          ,tipo_agencia
      from {{ ref('stg_erp__agencias') }}


),
   colaboradores as (

    select 
     id_colaborador
       , nome_completo
       , email
       , cpf
       , data_nascimento
       , endereco
       , cep
      from {{ ref('stg_erp__colaboradores') }}


),
  agencia_colabora as (
       select 
          id_agencia
          ,id_colaborador
       from {{ ref('int_agencia_colabora') }}


  ),
     --CTE para juntar colaborador e agência, criando uma view de relacionamneto é evitar duplicatas 
      contas_agencias_colaboradores as (
        select 
         c.id_num_conta
            ,c.id_agencia
            ,ac.id_colaborador
             ,md5(
              cast(coalesce(c.id_num_conta,'dbt_utils.generate_surrogate_key_null') as string )   
             || '-' ||  
             cast(coalesce( ac.id_colaborador, 'dbt_utils.generate_surrogate_key_null') as string  )
              ) as id_surrogate 
        from contas as c
          left join agencia_colabora as ac
            on c.id_agencia = ac.id_agencia
         
         
),

  transacoes_enriched as (
          select 
            t.id_transacao
            ,cac.id_surrogate 
            ,t.id_num_conta
            ,ct.id_cliente
            ,c.nome_completo as nome_clientes
            ,t.data_transacao
            ,t.nome_transacao
            ,t.valor_transacao
            ,ct.tipo_conta
            ,ct.data_abertura
            ,ct.saldo_total
            ,ct.saldo_disponivel
            ,ct.data_ultimo_lancamento
            ,cac.id_agencia
            ,cac.id_colaborador
            ,col.nome_completo  as colaborador
            


                -- dia  da semana (1 = domingo,  7 = sábado )
        ,extract(dayofweek from t.data_transacao) as dia_semana

       -- mês par ou ímpar

        ,case
            when mod(extract(month from t.data_transacao), 2) = 0
            then 'par'
            else 'impar'
        end as tipos_mes
        
        -- flag últimos 6 meses
        ,case
           when date(t.data_transacao) >= date_sub(date((select max(data_transacao) from transacoes)), interval 6 month)
           then 1
           else 0
         end as fl_ultimos_6m
        
        
        -- direção da transação (entrada / saque)
        ,case
           when t.valor_transacao  >= 0 then 'entrada'
           else 'saque'
         end as direcao_transacao

      
      
      
          from transacoes as   t
          left join contas as ct
             on t.id_num_conta = ct.id_num_conta

          left  join  contas_agencias_colaboradores  as  cac
            on t.id_num_conta = cac.id_num_conta 
          left join clientes as c 
             on ct.id_cliente = c.id_cliente  
         left join agencias as a
              on cac.id_agencia = a.id_agencia
         left join colaboradores as col 
              on cac.id_colaborador  = col.id_colaborador
     
)
   select * from transacoes_enriched





