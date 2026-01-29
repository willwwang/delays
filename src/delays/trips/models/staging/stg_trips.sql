select
    updated_at,
    trip_id,
    start_date,
    route_id,
    direction,
    location,
    location_status,
    departure_time,
    underway,
    train_assigned,
    last_position_update,
    current_stop_sequence_index,
    num_stops_left,
    has_delay_alert
from raw_trips
