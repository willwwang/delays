with (

    select
        stg_realtime__trips.updated_at,

    from {{ ref('stg_realtime__trips') }}
    left join {{ ref('stg_realtime__stop_time_updates') }} on
        stg_realtime__trips.

)


