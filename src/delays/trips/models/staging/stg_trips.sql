select
    updated_at,
    trip_id,
    start_date,
    route_id,
    direction,
    regexp_replace(location, r'[NS]$', '') as stop_id,
    location_status,
    headsign_text,
    departure_time,
    underway,
    train_assigned,
    last_position_update,
    current_stop_sequence_index,
    num_stops_left,
    has_delay_alert
from {{ source('raw', 'raw_trips') }}
where
    underway is true
