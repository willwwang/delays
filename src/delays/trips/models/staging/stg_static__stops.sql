select
    stop_id,
    stop_name
from {{ source('raw', 'raw_static__stops') }}
where
    location_type = 1
