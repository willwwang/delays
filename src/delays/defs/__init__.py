import dagster as dg
import duckdb
import polars as pl

from nyct_gtfs import NYCTFeed

def build_trips_dataframe(trips, measured_at):
    return pl.DataFrame(
        {
            "measured_at": measured_at,
            "service": [trip.route_id for trip in trips],
            "direction": [trip.direction for trip in trips],
            "current_stop": [
                trip.stop_time_updates[0].stop_name if trip.stop_time_updates else None
                for trip in trips
            ],
            "stops_left": [
                len(trip.stop_time_updates) if trip.stop_time_updates else 0
                for trip in trips
            ],
            "time_to_destination": [
                (trip.stop_time_updates[-1].arrival - measured_at).total_seconds()
                if trip.stop_time_updates else None
                for trip in trips
            ]
        }
    )

@dg.asset
def trains():
    feed = NYCTFeed("1")
    trips = feed.trips

    if not trips:
        return

    trips_df = build_trips_dataframe(trips, feed.last_generated)

    with duckdb.connect("mta.duckdb") as conn:
        conn.execute(
            """
            create table if not exists trips (
                measured_at timestamp,
                service varchar,
                direction varchar,
                current_stop varchar,
                stops_left int,
                time_to_destination int
            );
            """
        )
        conn.execute(
            """
            insert into trips select * from trips_df;
            """
        )
    
    return
