select
    stg_realtime__trips.updated_at,
    stg_realtime__trips.route_id,
    stg_realtime__trips.direction,
    stg_realtime__trips.location_status,
    stg_static__stops.stop_name,
    stg_realtime__trips.headsign_text as destination,
    stg_realtime__trips.last_position_update
from {{ ref("stg_realtime__trips") }}
left join {{ ref("stg_static__stops") }} on stg_realtime__trips.stop_id = stg_static__stops.stop_id
qualify
    stg_realtime__trips.updated_at = max(stg_realtime__trips.updated_at) over (partition by stg_realtime__trips.service_id)
