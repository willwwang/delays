import dagster as dg
import io
import pandas as pd
import requests
import zipfile

from nyct_gtfs import NYCTFeed

TRIP_COLUMNS = {
    "updated_at": "timestamp",
    "trip_id": "varchar",
    "nyc_train_id": "varchar",
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
    "start_date": "date",
    "stop_id": "varchar",
    "arrival": "timestamp",
    "departure": "timestamp",
    "scheduled_track": "varchar",
    "actual_track": "varchar"
}

ALERT_COLUMNS = {
    "alert_id": "varchar",
    "created_at": "timestamp",
    "header_text": "varchar"
}

ALERT_ENTITY_COLUMNS = {
    "alert_id": "varchar",
    "route_id": "varchar"
}

STATIC_TABLES = {
    "raw_static__stops": "stops.txt",
    "raw_static__stop_times": "stop_times.txt",
    "raw_static__trips": "trips.txt"
}


five_min_partitions = dg.TimeWindowPartitionsDefinition(
    start="2026-01-01-00:00",
    cron_schedule="*/5 * * * *",  # Every 5 minutes
    fmt="%Y-%m-%d-%H:%M",
    end_offset=1
)


def extract_trips_data(trips, updated_at) -> list[tuple]:
    return [
        (
            updated_at,
            trip.trip_id,
            trip.nyc_train_id,
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

def extract_stop_time_update_data(update, trip_id, start_date, updated_at) -> tuple:
    return (
        updated_at,
        trip_id,
        start_date,
        update.stop_id,
        update.arrival,
        update.departure,
        update.scheduled_track,
        update.actual_track
    )

def access_static_gtfs(url: str) -> bytes:
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def extract_static_gtfs(zip: bytes, tables: list[str]) -> pd.DataFrame:
    table_data = []
    with zipfile.ZipFile(io.BytesIO(zip)) as zf:
        for table in tables:
            table_data.append(zf.read(table))

    table_dfs = [pd.read_csv(io.BytesIO(data)) for data in table_data]
    return table_dfs

def extract_alert_data(alerts: dict) -> list[tuple]:
    return [
        (
            alert["id"],
            alert["alert"]["transit_realtime.mercury_alert"]["created_at"],
            [
                translation["text"]
                for translation in alert["alert"]["header_text"]["translation"]
                if translation["language"] == "en"
            ][0]
        )
        for alert in alerts if "alert" in alert["id"]
    ]

def extract_alert_entity_data(alert_entities: dict, alert_id) -> list[tuple]:
    return [
        (
            alert_id,
            alert_entity["route_id"]
        ) for alert_entity in alert_entities if "route_id" in alert_entity
    ]
    

@dg.multi_asset(
    outs={table: dg.AssetOut() for table in STATIC_TABLES}
)
def static():
    url = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"
    tables = STATIC_TABLES
    zip_bytes = access_static_gtfs(url)
    
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for table_name in tables:
            table_data = zf.read(tables[table_name])
            table_df = pd.read_csv(io.BytesIO(table_data))
            yield dg.Output(table_df, output_name=table_name)

@dg.multi_asset(
    outs={
        "raw_realtime__trips": dg.AssetOut(
            metadata={"partition_expr": "updated_at"}
        ),
        "raw_realtime__stop_time_updates": dg.AssetOut(
            metadata={"partition_expr": "updated_at"}
        )
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
            updates = [
                extract_stop_time_update_data(stop_time_update, trip.trip_id, trip.start_date, feed.last_generated)
                for stop_time_update in trip.stop_time_updates
            ]
            trip_updates.extend(updates)
    
    raw_realtime__trips = pd.DataFrame(
        data=all_trips,
        columns=TRIP_COLUMNS.keys()
    )

    raw_realtime__stop_time_updates = pd.DataFrame(
        data=trip_updates,
        columns=UPDATE_COLUMNS.keys()
    )

    return raw_realtime__trips, raw_realtime__stop_time_updates


# @dg.multi_asset(
#     outs={
#         "raw_alerts": dg.AssetOut(
#             metadata={"partition_expr": "updated_at"}
#         ),
#         "raw_alert_entities": dg.AssetOut(
#             metadata={"partition_expr": "updated_at"}
#         )
#     },
#     partitions_def=five_min_partitions
# )
# def alerts():
#     url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json"
#     alerts_response = requests.get(url)
#     alerts_response.raise_for_status()

#     alerts = alerts_response.json()
#     alert_data = extract_alert_data(alerts["entity"])
    
#     alert_entities_data = []
#     for alert in alerts["entity"]:
#         alert_entities_data.extend(extract_alert_entity_data(alert["alert"]["informed_entity"], alert["id"]))

#     raw_alerts = pd.DataFrame(
#         data=alert_data,
#         columns=ALERT_COLUMNS.keys()
#     )

#     raw_alert_entities = pd.DataFrame(
#         data=alert_entities_data,
#         columns=ALERT_ENTITY_COLUMNS.keys()
#     )

#     return raw_alerts, raw_alert_entities


# Define the asset job
partitioned_asset_job = dg.define_asset_job("partitioned_job", selection=[trains])

# This schedule will run every 5 minutes
asset_partitioned_schedule = dg.build_schedule_from_partitioned_job(
    partitioned_asset_job
)
