with 

source as (

    select * from {{ source('erp', 'agencias') }}

),

renamed as (

    select
        cast(cod_agencia  as string)                as id_agencia
        ,cast(nome as string )                      as nome_agencia
        ,cast(endereco  as string)                  as endereco 
        ,cast(cidade  as string)                    as cidade
        ,cast(uf  as string)                        as estado
        ,Format_date( '%d/%m/%Y',data_abertura )    as data_abertura 
        ,cast(tipo_agencia  as string)              as tipo_agencia
        ,row_number() over( partition by  cod_agencia order by cod_agencia) as  row_number
    from source
    where cod_agencia  is not null 

)

select 
 id_agencia
  ,nome_agencia
  ,endereco 
  ,cidade
  ,estado
  ,data_abertura
  ,tipo_agencia
from renamed
where row_number = 1