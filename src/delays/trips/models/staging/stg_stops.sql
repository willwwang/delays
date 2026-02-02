select
    stop_id,
    stop_name
from raw_stops
where
    location_type = 1
