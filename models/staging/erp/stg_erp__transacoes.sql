with 

source as (

    select * from {{ source('erp', 'transacoes') }}

),

renamed as (

    select
         cast(cod_transacao as string)                as id_transacao
        , cast(num_conta as string)                   as id_num_conta
        , cast(data_transacao as date)                as data_transacao 
        , cast(nome_transacao as string)  as nome_transacao
        
        
        ,cast(
            replace (
                 regexp_replace(
                  trim(cast(valor_transacao as string)), 
                  '[^0-9.-]',''),
                  ',' , '.') as numeric)  as valor_transacao

        ,row_number() over( partition by cod_transacao order by cod_transacao) as  row_number
    from source
    where cod_transacao  is not null 

)

select 
  id_transacao
  ,id_num_conta
  ,data_transacao 
  ,nome_transacao
  ,valor_transacao
from renamed
where row_number = 1


