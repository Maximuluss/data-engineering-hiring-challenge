{#
    Product (fct_task_weather_risk) and Ops (fct_location_daily_weather) must
    report the same number of scheduled and high-risk tasks per location and
    local day. Returns the location/days where they disagree, including tasks
    that fall on a day missing from the daily mart.
#}

with from_tasks as (

    select
        location_id,
        scheduled_date_local as forecast_date_local,
        count(*) as scheduled_task_count,
        count(*) filter (where weather_risk_label = 'high') as high_risk_task_count

    from {{ ref('fct_task_weather_risk') }}

    where scheduled_date_local is not null

    group by 1, 2

),

from_daily as (

    select location_id, forecast_date_local, scheduled_task_count, high_risk_task_count
    from {{ ref('fct_location_daily_weather') }}
    where scheduled_task_count > 0

)

select *
from from_tasks
full outer join from_daily using (location_id, forecast_date_local)
where from_tasks.scheduled_task_count is distinct from from_daily.scheduled_task_count
   or from_tasks.high_risk_task_count is distinct from from_daily.high_risk_task_count
