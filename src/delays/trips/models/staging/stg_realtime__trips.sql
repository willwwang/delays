select
    updated_at,
    trip_id as gtfs_trip_id,
    regexp_replace(trip_id, r'.*_', '') as shape_id,
    start_date,
    case
        when extract(dayofweek from start_date) = 1 then 'Sunday'
        when extract(dayofweek from start_date) between 2 and 6 then 'Weekday'
        when extract(dayofweek from start_date) = 7 then 'Saturday'
    end as day_of_week,
    route_id,
    direction,
    regexp_replace(location, r'[NS]$', '') as stop_id,
    replace(lower(location_status), '_', ' ') as location_status,
    headsign_text as destination,
    departure_time,
    underway as is_underway,
    train_assigned as has_train_assigned,
    last_position_update,
    current_stop_sequence_index + 1 as stop_sequence,
    num_stops_left,
    has_delay_alert
from {{ source('raw', 'raw_realtime__trips') }}
where
    is_underway is true
