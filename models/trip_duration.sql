{{
  config(
    materialized='view'
  )
}}

with trips as (

    select * from {{ ref('stg_trips') }}

),
/*  -- weather not needed
weather as (

    select * from {{ ref('stg_weather') }}

),
*/
final as (

        select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)", 
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)" 
from trips
group by 1 order by 1

)

select * from final