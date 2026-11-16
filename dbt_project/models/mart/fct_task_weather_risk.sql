{#
    One row per task with upcoming scheduled work: state = 'Assigned' and
    scheduled_at in the future. Cancelled tasks keep a scheduled_at but no work
    will happen, so they are excluded.

    Forecast hour: precipitation, precipitation probability and gusts describe
    the hour *preceding* their timestamp, so the task uses the one forecast row
    with scheduled_at < forecasted_at <= scheduled_at + 1h, i.e. the hour that
    contains the scheduled start. A range join rather than an exact hour match
    because half-hour-offset locations (Adelaide, Darwin) have forecast rows at
    :30 UTC while tasks are scheduled on the hour.

    Score (0-100) = category weather_sensitivity x worst weather factor, where
    each factor is scaled to 0-100:
      - rain chance:  precipitation_probability_pct as is
      - rain amount:  0 mm/h -> 0, 5 mm/h or more -> 100 (linear)
      - wind gusts:   20 km/h or less -> 0, 70 km/h or more -> 100 (linear)
      - conditions:   thunderstorm codes (95-99) -> 100, snow codes -> 80, else 0
    greatest() skips nulls, so a missing factor (e.g. UK rain chance past the
    model horizon) does not blank the score. Tasks with no forecast row at all
    (orphaned location, beyond the horizon) get a null score and label 'unknown'.
#}

with tasks as (

    select * from {{ ref('stg_tasks') }}

),

locations as (

    select * from {{ ref('stg_locations') }}

),

sensitivity as (

    select * from {{ ref('category_weather_sensitivity') }}

),

hourly as (

    select * from {{ ref('int_hourly_weather_forecast') }}

),

upcoming as (

    select *
    from tasks
    where state = 'Assigned'
      and scheduled_at > timezone('UTC', current_timestamp)

),

joined as (

    select
        upcoming.task_id,
        upcoming.title,
        upcoming.category,
        upcoming.location_id,
        locations.timezone,
        upcoming.scheduled_at,
        timezone(locations.timezone, timezone('UTC', upcoming.scheduled_at)) as scheduled_at_local,
        timezone(locations.timezone, timezone('UTC', upcoming.scheduled_at))::date as scheduled_date_local,
        coalesce(sensitivity.weather_sensitivity, 0) as weather_sensitivity,
        hourly.forecast_hour_id,
        hourly.fetched_at as forecast_fetched_at,
        hourly.temperature,
        hourly.weather_code,
        hourly.precipitation,
        hourly.precipitation_probability_pct,
        hourly.wind_gusts_10m_kmh

    from upcoming

    left join locations
        on upcoming.location_id = locations.location_id

    left join sensitivity
        on upcoming.category = sensitivity.category
        
    left join hourly
        on upcoming.location_id = hourly.location_id
        and hourly.forecasted_at > upcoming.scheduled_at
        and hourly.forecasted_at <= upcoming.scheduled_at + interval 1 hour

),

factors as (

    select
        *,
        precipitation_probability_pct::double as rain_chance_factor,
        least(precipitation / 5.0, 1) * 100 as rain_amount_factor,
        greatest(least((wind_gusts_10m_kmh - 20) / 50.0, 1), 0) * 100 as wind_factor,
        case
            when weather_code between 95 and 99 then 100
            when weather_code between 71 and 77 or weather_code in (85, 86) then 80
            when weather_code is not null then 0
        end as conditions_factor

    from joined

),

scored as (

    select
        *,
        case
            when forecast_hour_id is not null then round(
                weather_sensitivity
                * greatest(rain_chance_factor, rain_amount_factor, wind_factor, conditions_factor),
                1
            )
        end as weather_risk_score

    from factors

),

final as (

    select
        task_id,
        title,
        category,
        location_id,
        timezone,
        scheduled_at,
        scheduled_at_local,
        scheduled_date_local,
        forecast_fetched_at,
        temperature,
        weather_code,
        precipitation,
        precipitation_probability_pct,
        wind_gusts_10m_kmh,
        weather_sensitivity,
        rain_chance_factor,
        rain_amount_factor,
        wind_factor,
        conditions_factor,
        weather_risk_score,
        case
            when weather_risk_score is null then 'unknown'
            when weather_risk_score >= {{ var('weather_risk_high_threshold') }} then 'high'
            when weather_risk_score >= {{ var('weather_risk_medium_threshold') }} then 'medium'
            else 'low'
        end as weather_risk_label

    from scored

)

select * from final
