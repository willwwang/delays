import dagster as dg
import duckdb
import polars as pl

from nyct_gtfs import NYCTFeed

TRIP_COLUMNS = {
    "updated_at": "timestamp",
    "trip_id": "varchar",
    "start_date": "date",
    "route_id": "varchar",
    "direction": "varchar",
    "location": "varchar",
    "location_status": "varchar",
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

def extract_trips_data(trips, updated_at):
    return [
        (
            updated_at,
            trip.trip_id,
            trip.start_date,
            trip.route_id,
            trip.direction,
            trip.location,
            trip.location_status,
            trip.departure_time,
            trip.underway,
            trip.train_assigned,
            trip.last_position_update,
            trip.current_stop_sequence_index,
            len(trip.stop_time_updates),
            trip.has_delay_alert
        ) for trip in trips
    ]

def extract_stop_time_update_data(update, trip_id, updated_at):
    return (
        updated_at,
        trip_id,
        update.stop_id,
        update.arrival,
        update.departure
    )


@dg.asset
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
    
    trips_df = pl.DataFrame(
        data=all_trips,
        schema=TRIP_COLUMNS.keys(),
        orient="row"
    )
    updates_df = pl.DataFrame(
        data=trip_updates,
        schema=UPDATE_COLUMNS.keys(),
        orient="row"
    )

    with duckdb.connect("mta.duckdb") as conn:
        conn.execute(
            f"""
            create table if not exists raw_trips (
                {', '.join([f"{col} {TRIP_COLUMNS[col]}" for col in TRIP_COLUMNS])}
            );
            create table if not exists raw_stop_time_updates (
                {', '.join([f"{col} {UPDATE_COLUMNS[col]}" for col in UPDATE_COLUMNS])}
            );
            """
        )
        conn.execute(
            """
            insert into raw_trips select * from trips_df;
            insert into raw_stop_time_updates select * from updates_df;
            """
        )
