select
    updated_at,
    trip_id,
    regexp_extract(trip_id, r'\.([^.]+)$') as stopping_pattern,
    start_date,
    route_id as service_id,
    direction,
    regexp_replace(location, r'[NS]$', '') as stop_id,
    replace(lower(location_status), '_', ' ') as location_status,
    headsign_text as destination,
    departure_time,
    underway as is_underway,
    train_assigned as has_train_assigned,
    last_position_update,
    current_stop_sequence_index as num_stops_passed,
    num_stops_left,
    has_delay_alert
from {{ source('raw', 'raw_realtime__trips') }}
where
    is_underway is true
