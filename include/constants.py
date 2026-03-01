# ── API URLs ──────────────────────────────────────────────────────────────────
NYC_311_API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

WEATHER_API_URL = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude=40.7128&longitude=-74.0060"
    "&daily=temperature_2m_max,temperature_2m_min"
    "&timezone=America/New_York"
)

# ── Airflow connection IDs ───────────────────────────────────────────────────
POSTGRES_CONN_ID = "postgres_conn"

# ── Database schemas ─────────────────────────────────────────────────────────
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# ── Table names ──────────────────────────────────────────────────────────────
TAXI_TABLE = "taxi_data"
WEATHER_TABLE = "weather_data"
