with 

source as (

    select * from {{ source('erp', 'contas') }}

),

renamed as (

    select
        cast(num_conta as string)                                       as  id_num_conta
        , cast(cod_cliente as string)                                   as  id_cliente      
        , cast(cod_agencia as string)                                   as  id_agencia
        , cast(cod_colaborador as string)                               as  colaborador 
        , cast(tipo_conta as string)                                    as  tipo_conta
        , format_date('%d/%m/%Y' , data_abertura)                       as  data_abertura
        , cast(saldo_total as 	bignumeric )                            as  saldo_total
        , cast(saldo_disponivel as 	bignumeric )                        as  saldo_disponivel
        , format_date('%d/%m/%Y' ,data_ultimo_lancamento)               as  data_ultimo_lancamento
        ,row_number() over( partition by num_conta order by num_conta)  as  row_number        
    from source
      where num_conta is not null

)

select 
 id_num_conta
 ,id_cliente      
 ,id_agencia
 ,colaborador
 ,tipo_conta
 ,data_abertura
 ,saldo_total
 ,saldo_disponivel
 ,data_ultimo_lancamento

from renamed
where row_number = 1
