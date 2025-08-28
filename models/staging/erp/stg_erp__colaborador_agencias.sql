with 

source as (

    select * from {{ source('erp', 'colaborador_agencia') }}

),

renamed as (

    select
      cast(cod_agencia as string)        as id_agencia
       , cast(cod_colaborador as string) as id_colaborador 
        -- Aplicando a função de janela para não ter ids em duplicidades 
        ,row_number() over( partition by  cod_colaborador, cod_agencia order by cod_colaborador) as  row_number
    from source
    where cod_colaborador  is not null and cod_agencia is not null 
)

select 
 id_agencia
 ,id_colaborador
from renamed
where row_number = 1