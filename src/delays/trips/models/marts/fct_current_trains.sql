select
    stg_trips.updated_at,
    stg_trips.route_id,
    stg_trips.direction,
    stg_stops.stop_name,
    stg_trips.headsign_text,
    stg_trips.last_position_update
from {{ ref("stg_trips") }}
left join {{ ref("stg_stops") }} on stg_trips.stop_id = stg_stops.stop_id
where
    stg_trips.underway is true
qualify
    stg_trips.updated_at = max(stg_trips.updated_at) over (partition by stg_trips.route_id)
