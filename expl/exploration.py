import requests
import zipfile
import io

import pandas as pd

def access_static_gtfs(url: str) -> bytes:
    response = requests.get(url)
    response.raise_for_status()
    return response.content

url = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"

zip_bytes = access_static_gtfs(url)

with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
    # List all files in the zip
    print(zf.namelist())  # ['stops.txt', 'routes.txt', 'trips.txt', ...]
    
    # Read a specific file
    stops_data = zf.read("stops.txt")  # Returns bytes

    stops_df = pd.read_csv(io.StringIO(stops_data))