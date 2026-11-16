with source as (

    select * from {{ source('raw', 'raw_weather_forecast') }}

),

flattened as (

    select
        location_id,
        fetched_at,
        (response ->> 'latitude')::double as latitude,
        (response ->> 'longitude')::double as longitude,
        (response ->> 'elevation')::double as elevation,
        response ->> 'timezone' as timezone,
        (response ->> 'utc_offset_seconds')::int as utc_offset_seconds,
        -- unixtime epoch seconds -> naive UTC timestamp, same convention as stg_tasks
        timezone('UTC', to_timestamp(unnest((response -> '$.hourly.time')::bigint[]))) as forecasted_at,
        unnest((response -> '$.hourly.weather_code')::int[]) as weather_code,
        unnest((response -> '$.hourly.temperature_2m')::double[]) as temperature,
        -- the three below cover the hour *preceding* forecasted_at
        unnest((response -> '$.hourly.precipitation')::double[]) as precipitation,
        unnest((response -> '$.hourly.precipitation_probability')::int[]) as precipitation_probability_pct,
        unnest((response -> '$.hourly.wind_gusts_10m')::double[]) as wind_gusts_10m_kmh

    from source

),

{#
    raw_weather_forecast is append-only and fetch windows overlap, so the same
    location/hour appears once per fetch. Keep only the latest fetch per hour to
    give one row per (location_id, forecasted_at); otherwise joins to tasks fan out
    and daily sums double count. Forecast history stays in raw for run-to-run comparisons.
#}
filtered as (

    select *

    from flattened

    qualify row_number() over (partition by location_id, forecasted_at order by fetched_at desc) = 1

)

select * from filtered
