select
from raw_stop_time_updates
left join raw_stops on
    raw_stop_time_updates.stop_id = raw_stops.stop_id and
    raw_stop_time_updates.
where
    raw_stops.location_type 
