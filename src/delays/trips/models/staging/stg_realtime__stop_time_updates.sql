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
    regexp_replace(stop_id, r'[NS]$', '') as stop_id,
    arrival,
    departure,
    scheduled_track,
    actual_track
from {{ source('raw', 'raw_realtime__stop_time_updates') }}
