import pytest
import polars as pl
from datetime import datetime, timedelta
from dataclasses import dataclass

# Mock objects to simulate NYCTFeed data
@dataclass
class MockStopTimeUpdate:
    stop_name: str
    arrival: datetime

@dataclass  
class MockTrip:
    route_id: str
    direction: str
    stop_time_updates: list[MockStopTimeUpdate]

def test_build_trips_dataframe():
    from delays.defs import build_trips_dataframe
    
    now = datetime.now()
    
    # Create mock trips
    trips = [
        MockTrip(
            route_id="1",
            direction="N",
            stop_time_updates=[
                MockStopTimeUpdate("14 St", now + timedelta(minutes=2)),
                MockStopTimeUpdate("Times Sq", now + timedelta(minutes=5)),
            ]
        ),
    ]
    
    df = build_trips_dataframe(trips, now)
    
    assert len(df) == 1
    assert df["service"][0] == "1"
    assert df["direction"][0] == "N"
    assert df["stops_left"][0] == 2
