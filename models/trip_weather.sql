{{
  config(
    materialized='view'
  )
}}

with trips as (

    select * from {{ ref('stg_trips') }}

),

weather as (

    select * from {{ ref('stg_weather') }}

),

final as (

        select   weather as conditions
        ,count(*) as num_trips
from     trips 
left 
join     weather
  on     date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where     conditions is not null
group by 1 order by 2 desc

)

select * from final