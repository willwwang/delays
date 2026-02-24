select
    route_id,
    regexp_extract(trip_id, r'_(\d{6}_.*)$') as gtfs_trip_id,
    service_id as day_of_week,
    trip_headsign as destination,
    case
        when direction_id = 0 then 'N'
        when direction_id = 1 then 'S'
        else 0
    end as direction,
    shape_id
from {{ source('raw', 'raw_static__trips') }}
