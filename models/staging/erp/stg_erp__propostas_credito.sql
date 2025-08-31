with 

source as (

    select * from {{ source('erp', 'propostas_credito') }}

),

renamed as (

    select
        cast(cod_proposta as string)                                          as id_proposta
        ,cast(cod_cliente as string)                                          as id_cliente
        ,cast(cod_colaborador as string)                                      as id_colaborador
        ,cast(data_entrada_proposta as date)                                  as data_entrada_proposta
       
        ,cast(
            replace (
                 regexp_replace(
                  trim(cast(valor_proposta as string)), 
                  '[^0-9.-]',''),
                  ',' , '.') as numeric)                                       as valor_proposta

         ,cast(
            replace (
                 regexp_replace(
                  trim(cast(valor_financiamento as string)), 
                  '[^0-9.-]',''),
                  ',' , '.') as numeric)                                       as valor_financiamento

        ,cast(
            replace (
                 regexp_replace(
                  trim(cast(valor_entrada as string)), 
                  '[^0-9.-]',''),
                  ',' , '.') as numeric)                                       as valor_entrada 
        ,cast(
            replace (
                 regexp_replace(
                  trim(cast(valor_prestacao as string)), 
                  '[^0-9.-]',''),
                  ',' , '.') as numeric)                                       as valor_prestacao

         ,cast(taxa_juros_mensal as numeric)                                    as taxa_juros_mensal
        ,cast(quantidade_parcelas as string)                                   as quantidade_parcelas
       ,cast(carencia as string)                                              as carencia 
       ,cast(status_proposta as string)                                       as status_proposta 
        ,row_number() over( partition by  cod_proposta order by cod_proposta) as  row_number
   from source
   where cod_proposta  is not null 
)
select 
id_proposta
  ,id_cliente
  ,id_colaborador
  ,data_entrada_proposta
  ,taxa_juros_mensal
  ,cast(round( valor_proposta, 2) as string)       as valor_proposta
  ,cast(round( valor_financiamento, 2) as string)  as  valor_financiamento
  ,cast(round( valor_entrada, 2) as string)        as valor_entrada
  ,cast(round( valor_prestacao, 2) as string)      as valor_prestacao
  ,quantidade_parcelas
  ,carencia 
  ,status_proposta 
from renamed
where row_number = 1