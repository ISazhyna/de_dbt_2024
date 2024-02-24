{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    row_number() over(partition by pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
)
select
    -- identifiers
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_location_id,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_location_id,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
from tripdata
where rn = 1

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}