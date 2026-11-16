{#
    One row per location per forecast hour, with the hour expressed in the
    location's local time. Local time is derived from the IANA timezone per row
    (not the fixed utc_offset_seconds), so hours either side of a DST change land
    on the correct local day.
#}

with forecast as (

    select * from {{ ref('stg_weather_forecast') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['location_id', 'forecasted_at']) }} as forecast_hour_id,
        location_id,
        fetched_at,
        timezone,
        forecasted_at,
        timezone(timezone, timezone('UTC', forecasted_at)) as forecasted_at_local,
        timezone(timezone, timezone('UTC', forecasted_at))::date as forecast_date_local,
        weather_code,
        temperature,
        precipitation,
        precipitation_probability_pct,
        wind_gusts_10m_kmh

    from forecast

)

select * from final
