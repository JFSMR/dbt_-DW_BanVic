
-- importando dados tabela  transacoes
with

 
 transacoes as (
 select 
  id_transacao
    ,id_num_conta
    , data_transacao
    ,nome_transacao
    ,coalesce(valor_transacao, 0 ) as valor_transacao
  from {{ ref('stg_erp__transacoes') }} 
 )
 ,
 -- importando dados tabela   contas
      contas as (
      select 
         id_num_conta
         ,id_cliente      
         ,id_agencia
         ,id_colaborador
         ,tipo_conta
         ,data_abertura          
         ,coalesce(saldo_total, 0) as saldo_total
         ,coalesce(saldo_disponivel, 0) as saldo__disponivel
         ,data_ultimo_lancamento

 
     from {{ ref('stg_erp__contas') }}

)
 ,
 -- importando dados tabela   crédito

  propostas as (
     select 
     id_proposta
       ,id_cliente
       ,id_colaborador
       ,data_entrada_proposta
       ,taxa_juros_mensal
       ,valor_proposta
       ,valor_financiamento
       ,valor_entrada 
       ,valor_prestacao
       ,quantidade_parcelas
       ,carencia 
       ,status_proposta 
     from {{ ref('stg_erp__propostas_credito') }}
),

transacoes_enriched as (
    select
        t.id_transacao
       ,t.id_num_conta
       ,t.data_transacao
       ,t.nome_transacao
       ,t.valor_transacao
       ,c.id_cliente      
       ,c.id_agencia
       ,c.id_colaborador
       ,c.tipo_conta
       ,c.data_abertura          
       ,c.saldo_total
       ,c.saldo__disponivel
       ,c.data_ultimo_lancamento
       ,p.id_proposta
       ,p.data_entrada_proposta
       ,p.taxa_juros_mensal
       ,p.valor_proposta
       ,p.valor_financiamento
       ,p.valor_entrada 
       ,p.valor_prestacao
       ,p.quantidade_parcelas
       ,p.carencia 
       ,p.status_proposta 

        -- Adicionar um número de linha para lidar com a duplicidade de proposta por cliente
        -- isso garante que uma única linha de transação não seja duplicada

        ,row_number() over(
             partition by t.id_transacao 
             order by p.data_entrada_proposta desc
        )  as  rn_proposta
    from transacoes as t
    left     join contas as c
     on  t.id_num_conta = c.id_num_conta
    inner   join propostas as p
         on c.id_cliente = p.id_cliente
         and c.id_colaborador = p.id_colaborador

)

   select
      md5(
              cast(coalesce(id_transacao,'') as string )   
             || '-' ||  
             cast(coalesce( id_proposta,'') as string  )
              ) as id_surrogate 

               ,id_transacao
               ,id_num_conta
               ,data_transacao
               ,nome_transacao
               ,valor_transacao
               ,id_cliente      
               ,id_agencia
               ,id_colaborador
               ,tipo_conta
               ,data_abertura         
               ,saldo_total
               ,saldo__disponivel
               ,data_ultimo_lancamento
               ,status_proposta 
               ,id_proposta
               ,data_entrada_proposta
               ,taxa_juros_mensal

               -- metrica de proposta 
               ,valor_proposta
               ,valor_financiamento
               ,valor_entrada 
               ,valor_prestacao
               ,quantidade_parcelas
               ,carencia 

               -- metricas de transacao
                ,case when valor_transacao > 0 then  valor_transacao  else 0 end as  valor_receita_transacao
                ,case when valor_transacao < 0 then  valor_transacao  else 0 end as  valor_despesa_transacao
    from transacoes_enriched 
    where rn_proposta = 1
    