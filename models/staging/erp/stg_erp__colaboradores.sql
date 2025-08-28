with 

source as (

    select * from {{ source('erp', 'colaboradores') }}

),

renamed as (

    select
       cast(cod_colaborador as string)                    as id_colaborador
       ,concat(primeiro_nome, ' ', ultimo_nome)           as nome_completo
       ,cast(email as string)                             as email
       ,cast(cpf as string)                               as cpf
       ,format_date('%d/%m/%Y', data_nascimento)          as data_nascimento
       ,cast(endereco as string)                          as endereco
       ,cast(cep as string)                               as cep
       ,row_number() over( partition by  cod_colaborador order by cod_colaborador) as  row_number
    from source
    where cod_colaborador  is not null 
)

select 
 id_colaborador
 , nome_completo
 , email
 , cpf
 , data_nascimento
 , endereco
 , cep
from renamed
where row_number = 1
