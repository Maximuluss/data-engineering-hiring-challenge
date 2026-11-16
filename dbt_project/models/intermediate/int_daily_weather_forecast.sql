{#
    One row per location per local day. Recreates Open-Meteo's daily variables,
    which the docs describe as a simple 24 hour aggregation of the hourly values,
    grouped on the hourly timestamps 00:00-23:00 local time.

    weather_code: the API returns the most severe code of the day; WMO codes
    increase with severity, so max() is used.
    Nulls: the API returns null for a day when any hour of that variable is null
    (e.g. precipitation probability past the model horizon), so each aggregate is
    only kept when every hour of the day has a value.
    Temperature max/min will not match the API exactly: Open-Meteo derives its
    daily extremes from sub-hourly model data, so they are usually a few tenths
    beyond the hourly min/max.
    hour_count: normally 24 (23/25 on DST days); lower means the day is only
    partly covered by the forecast window.
#}

with hourly as (

    select * from {{ ref('int_hourly_weather_forecast') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['location_id', 'forecast_date_local']) }} as forecast_day_id,
        location_id,
        timezone,
        forecast_date_local,
        case when count(weather_code) = count(*) then max(weather_code) end as weather_code,
        case when count(temperature) = count(*) then max(temperature) end as max_temperature,
        case when count(temperature) = count(*) then min(temperature) end as min_temperature,
        case when count(precipitation) = count(*) then sum(precipitation) end as precipitation_sum,
        case when count(precipitation_probability_pct) = count(*)
            then max(precipitation_probability_pct) end as precipitation_probability_max_pct,
        case when count(wind_gusts_10m_kmh) = count(*) then max(wind_gusts_10m_kmh) end as wind_gusts_10m_max_kmh,
        count(*) as hour_count

    from hourly

    group by location_id, timezone, forecast_date_local

)

select * from final
