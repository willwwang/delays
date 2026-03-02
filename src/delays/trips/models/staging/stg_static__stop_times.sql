select
    trip_id,
    regexp_extract(trip_id, r'_(\d{6}_.*)$') as gtfs_trip_id,
    regexp_replace(trip_id, r'.*_', '') as shape_id,
    regexp_replace(location, r'[NS]$', '') as stop_id,
    arrival_time,
    departure_time,
    stop_sequence
from {{ source('raw', 'raw_static__stop_times') }}
