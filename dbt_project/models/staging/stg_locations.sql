with 

source as (

    select * from {{ ref('raw_locations') }}

),

final as (

    select
        location_id,
        city,
        country as country_code,
        lat::decimal(7,4) as latitude,
        lng::decimal(7,4) as longitude,
        timezone

    from source


)

select * from final
