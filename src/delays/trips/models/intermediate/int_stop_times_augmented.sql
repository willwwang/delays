select
    stg_static__stop_times.*,
    stg_static__trips.route_id,
    stg_static__trips.day_of_week,
    stg_static__trips.direction
from {{ ref('stg_static__stop_times') }}
left join {{ ref('stg_static__trips') }} on stg_static__stop_times.trip_id = stg_static__trips.trip_id
