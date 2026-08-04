"""Static reference data shared across the pipeline.

Weather is fetched for a small, curated set of major NJ Transit rail hubs
rather than every stop. Each stop is later assigned to its nearest hub
(haversine) so the "on-time by weather" mart stays meaningful without
hammering the weather API once per stop.
"""

# name -> (latitude, longitude)
WEATHER_STATIONS = {
    "Newark Penn":    (40.7357, -74.1643),
    "Secaucus Jct":   (40.7616, -74.0757),
    "Hoboken":        (40.7348, -74.0277),
    "New Brunswick":  (40.4963, -74.4444),
    "Trenton":        (40.2181, -74.7546),
    "Summit":         (40.7165, -74.3574),
}

# Layout prefixes inside the data lake bucket.
BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"
QUARANTINE_PREFIX = "quarantine"

# Raw GTFS files we actually use downstream.
GTFS_STATIC_FILES = ["routes.txt", "trips.txt", "stops.txt", "stop_times.txt"]