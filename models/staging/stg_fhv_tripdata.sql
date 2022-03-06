{{ config(materialized='view') }}

with fhv_tripdata as
(
    select
    dispatching_base_num,
    PULocationID,
    DOLocationID,
    pickup_datetime,
    dropoff_datetime,
    SR_Flag,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
    from {{ source('staging','fhv_tripdata_external_table')}}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as trip_id,
    dispatching_base_num,
    cast(PULocationID as integer) as pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- flag
    cast(SR_Flag as integer) as sr_flag
from fhv_tripdata
