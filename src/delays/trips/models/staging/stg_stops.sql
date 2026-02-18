select
    stop_id,
    stop_name
from {{ source('raw', 'raw_stops') }}
where
    location_type = 1
