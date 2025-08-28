with 

source as (

    select * from {{ source('erp', 'transacoes') }}

),

renamed as (

    select
         cast(cod_transacao as string)                as id_transacao
        , cast(num_conta as string)                   as num_conta
        , format_date('%d/%m/%Y' ,data_transacao )    as data_transacao 
        , cast(nome_transacao as string)              as nome_transacao
        , cast(valor_transacao as bignumeric)         as valor_transacao
        ,row_number() over( partition by cod_transacao order by cod_transacao) as  row_number
    from source
    where cod_transacao  is not null 
)
select 
  id_transacao
  ,num_conta
  ,data_transacao 
  ,nome_transacao
  ,valor_transacao
from renamed
where row_number = 1
