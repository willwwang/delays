select
    trip_id,
    stop_id,
    arrival_time,
    departure_time,
    stop_sequence
from {{ source('raw', 'raw_stop_times') }}
    