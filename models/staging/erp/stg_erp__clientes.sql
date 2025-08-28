with 

source as (

    select * from {{ source('erp', 'clientes') }}

),

renamed as (

    select
         cast(cod_cliente as string)                       as id_cliente
        , concat(primeiro_nome, ' ', ultimo_nome)          as nome_completo
        , cast(email as string)                            as email 
        , cast(tipo_cliente as string)                     as tipo_cliente 
        , format_date('%d/%m/%Y' , data_inclusao)          as data_inclusao
        , cast(cpfcnpj as string)                          as cpfcnpj
        , format_date('%d/%m/%Y', data_nascimento)         as data_nascimento
        , cast(endereco as string)                         as endereco
        , cast(cep as string)                              as cep 
         ,row_number() over( partition by  cod_cliente order by cod_cliente) as  row_number
    from source
    where cod_cliente  is not null 

)

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
from renamed
where row_number = 1