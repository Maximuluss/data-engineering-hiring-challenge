with

source as (

    select * from {{ source('raw', 'raw_tasks') }}

),

staged as (

    select
        task_id,
        title,
        {{ initcap('category') }} as category,
        case
            when location_id in (select distinct location_id from {{ ref('stg_locations') }})
            then location_id
            else 'Missing Location'
        end as location_id,
        {{ str_to_utc('posted_at') }} as posted_at,
        {{ initcap('state') }} as state,
        replace(budget, '$', '')::decimal(10,2) as budget,
        {{ str_to_utc('assigned_at') }} as assigned_at,
        {{ str_to_utc('scheduled_at') }} as scheduled_at,
        row_number() over (partition by task_id order by posted_at desc) as no

    from source

),

final as (

    select
        task_id,
        title,
        category,
        location_id,
        posted_at,
        state,
        budget,
        assigned_at,
        scheduled_at

    from staged

    where no = 1

)

select * from final
