with
 colaboradores as (
  select * from  {{ ref('int_dim_colaboradores') }}

 )

 select * from colaboradores