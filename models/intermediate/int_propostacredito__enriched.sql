with
   -- Importar as tabelas para enriquecer os dados 
   
     proposta as (
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
   )
   ,
   clientes as (
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
   
   
   )
   ,
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
   )
   ,
   Cola_agencia as  (
   
    select 
     id_agencia
     ,id_colaborador
    from {{ ref('stg_erp__colaborador_agencias') }}
   
   )
   ,
   agencia as (
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
      
   -- joins para enriquecer os dados 
   propostas_enriquecidas as (
       select
        {{dbt_utils.generate_surrogate_key(['proposta.id_proposta'])}} as sk_proposta_credito
        
           ,proposta.id_proposta
           ,proposta.id_cliente
           ,proposta.id_colaborador
           ,proposta.data_entrada_proposta
           ,proposta.taxa_juros_mensal
           ,proposta.valor_proposta
           ,proposta.valor_financiamento
           ,proposta.valor_entrada 
           ,proposta.valor_prestacao
           ,proposta.quantidade_parcelas
           ,proposta.carencia 
           ,proposta.status_proposta
           ,clientes.tipo_cliente
           ,colaboradores.nome_completo as nome_colaboradores
           ,agencia.nome_agencia
           ,agencia.tipo_agencia
            
        ,case when lower(trim(proposta.status_proposta)) like '%aprovada%' then 1 else 0  end    as aprovada
        ,case when lower(trim(proposta.status_proposta)) like '%analise%' then 1  else   0  end      as analise
        ,case when lower(trim(proposta.status_proposta)) like '%enviada%' then 1 else   0  end      as enviado
        ,case when lower(trim(proposta.status_proposta)) like '%documento%' then 1 else 0  end    as val_documento
        




       from proposta
       left join clientes
          on proposta.id_cliente = clientes.id_cliente
       left join colaboradores
           on proposta.id_colaborador = colaboradores.id_colaborador
       left join cola_agencia
           on colaboradores.id_colaborador = cola_agencia.id_colaborador
       left join agencia 
           on cola_agencia.id_agencia =agencia.id_agencia       
   
          
   )
   --seleção final de cálculo das métricas
   select
     sk_proposta_credito
     ,id_proposta
     ,data_entrada_proposta
     ,status_proposta
     ,tipo_cliente
     ,nome_agencia
     ,tipo_agencia
     ,nome_colaboradores

     ,valor_proposta
     ,valor_financiamento
     ,valor_entrada 
     ,valor_prestacao
     ,quantidade_parcelas
     ,taxa_juros_mensal
     ,carencia 
     ,aprovada
     ,analise
     ,enviado
     ,val_documento 
     
      
   from propostas_enriquecidas
   

   