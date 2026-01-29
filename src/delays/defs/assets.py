import dagster as dg
import io
import pandas as pd
import requests
import zipfile

from nyct_gtfs import NYCTFeed

TRIP_COLUMNS = {
    "updated_at": "timestamp",
    "trip_id": "varchar",
    "start_date": "date",
    "route_id": "varchar",
    "direction": "varchar",
    "location": "varchar",
    "location_status": "varchar",
    "headsign_text": "varchar",
    "departure_time": "timestamp",
    "underway": "boolean",
    "train_assigned": "boolean",
    "last_position_update": "timestamp",
    "current_stop_sequence_index": "integer",
    "num_stops_left": "integer",
    "has_delay_alert": "boolean",
}

UPDATE_COLUMNS = {
    "updated_at": "timestamp",
    "trip_id": "varchar",
    "stop_id": "varchar",
    "arrival": "timestamp",
    "departure": "timestamp"
}

five_min_partitions = dg.TimeWindowPartitionsDefinition(
    start="2026-01-01-00:00",
    cron_schedule="*/5 * * * *",  # Every 5 minutes
    fmt="%Y-%m-%d-%H:%M",
)

def extract_trips_data(trips, updated_at) -> list[tuple]:
    return [
        (
            updated_at,
            trip.trip_id,
            trip.start_date,
            trip.route_id,
            trip.direction,
            trip.location,
            trip.location_status,
            trip.headsign_text,
            trip.departure_time,
            trip.underway,
            trip.train_assigned,
            trip.last_position_update,
            trip.current_stop_sequence_index,
            len(trip.stop_time_updates),
            trip.has_delay_alert
        ) for trip in trips
    ]

def extract_stop_time_update_data(update, trip_id, updated_at) -> tuple:
    return (
        updated_at,
        trip_id,
        update.stop_id,
        update.arrival,
        update.departure
    )

def access_static_gtfs(url: str) -> bytes:
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def extract_static_gtfs(zip: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(zip)) as zf:
        stops_data = zf.read("stops.txt")  # Returns bytes

    stops_df = pd.read_csv(io.BytesIO(stops_data))

    return stops_df


@dg.asset
def raw_stops():
    url = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"
    zip_bytes = access_static_gtfs(url)
    stops_df = extract_static_gtfs(zip_bytes)

    return stops_df


@dg.multi_asset(
    outs={
        "raw_trips": dg.AssetOut(),
        "raw_stop_time_updates": dg.AssetOut()
    },
    partitions_def=five_min_partitions
)
def trains():
    all_trips = []
    trip_updates = []

    for source in ["1", "A", "B", "G", "J", "L", "N", "SIR"]:
        feed = NYCTFeed(source)
        trips = feed.trips
        all_trips.extend(extract_trips_data(trips, feed.last_generated))
        for trip in trips:
            n_updates = len(trip.stop_time_updates)
            if n_updates == 0:
                continue
            elif n_updates == 1:
                updates = [
                    extract_stop_time_update_data(trip.stop_time_updates[0], trip.trip_id, feed.last_generated)
                ]
            else:
                updates = [
                    extract_stop_time_update_data(trip.stop_time_updates[i], trip.trip_id, feed.last_generated)
                    for i in [0, -1]
                ]
            trip_updates.extend(updates)
    
    raw_trips = pd.DataFrame(
        data=all_trips,
        columns=TRIP_COLUMNS.keys()
    )

    raw_stop_time_updates = pd.DataFrame(
        data=trip_updates,
        columns=UPDATE_COLUMNS.keys()
    )

    return raw_trips, raw_stop_time_updates
