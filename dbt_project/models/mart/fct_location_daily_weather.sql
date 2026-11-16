{#
    One row per location per local day of the forecast window (days cut at
    midnight in the location's own timezone).

    Task counts come from fct_task_weather_risk, so Ops and Product always see
    the same high-risk tasks for a location and day. Tasks whose location is
    missing from stg_locations have no local day and are not counted here.
#}

with daily as (

    select * from {{ ref('int_daily_weather_forecast') }}

),

locations as (

    select * from {{ ref('stg_locations') }}

),

task_counts as (

    select
        location_id,
        scheduled_date_local,
        count(*) as scheduled_task_count,
        count(*) filter (where weather_risk_label = 'high') as high_risk_task_count

    from {{ ref('fct_task_weather_risk') }}

    where scheduled_date_local is not null

    group by location_id, scheduled_date_local

),

final as (

    select
        daily.forecast_day_id as location_day_id,
        daily.location_id,
        locations.city,
        locations.country_code,
        daily.timezone,
        daily.forecast_date_local,
        coalesce(task_counts.scheduled_task_count, 0) as scheduled_task_count,
        coalesce(task_counts.high_risk_task_count, 0) as high_risk_task_count,
        daily.weather_code,
        {{ wmo_weather_description('daily.weather_code') }} as weather_description,
        daily.max_temperature,
        daily.min_temperature,
        daily.precipitation_sum,
        daily.precipitation_probability_max_pct,
        daily.wind_gusts_10m_max_kmh

    from daily
    left join locations
        on daily.location_id = locations.location_id
    left join task_counts
        on daily.location_id = task_counts.location_id
        and daily.forecast_date_local = task_counts.scheduled_date_local

)

select * from final
