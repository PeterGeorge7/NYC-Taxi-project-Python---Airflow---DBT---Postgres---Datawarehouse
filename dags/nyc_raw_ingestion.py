from airflow.sdk import dag, task
from datetime import datetime

from include.constants import (
    NYC_311_API_URL,
    WEATHER_API_URL,
    BRONZE_SCHEMA,
    TAXI_TABLE,
    WEATHER_TABLE,
)
from include.custom_logging import get_logger
from include.db import load_df_to_postgres
from include.utils import (
    stringfy_complex_json,
    get_only_needed_columns,
    get_dates_from_context,
)


@dag(
    dag_id="nyc_raw_ingestion_v3",
    start_date=datetime(2026, 2, day=20),
    schedule="@daily",
    catchup=True,
)
def nyc_raw_ingestion():

    @task()
    def ingest_311_data(**context):
        import pandas as pd
        from include.extract_nyc import ExtractTaxiNyc

        logger = get_logger("ingest_311_data")
        logger.info("Starting data ingestion from NYC 311 API...")

        start_date, end_date = get_dates_from_context(context["data_interval_end"])

        params = {
            "$limit": 5000,
            "$where": f"created_date >= '{start_date}' AND created_date < '{end_date}'",
        }
        try:
            taxi_data = ExtractTaxiNyc().extract_taxi_data(NYC_311_API_URL, params)

            if taxi_data.empty:
                logger.warning("No data found between %s and %s", start_date, end_date)
                return

            taxi_data = get_only_needed_columns(taxi_data)
            taxi_data = stringfy_complex_json(taxi_data)

            load_df_to_postgres(taxi_data, TAXI_TABLE, BRONZE_SCHEMA)

            logger.info(
                "Data ingestion completed successfully for data between %s and %s with %d records",
                start_date,
                end_date,
                len(taxi_data),
            )
        except Exception as e:
            logger.error("Data ingestion failed: %s", e)
            raise

    @task()
    def ingest_weather_data(**context):
        import pandas as pd
        from include.extract_weather import ExtractWeatherNyc

        logger = get_logger("ingest_weather_data")
        logger.info("Starting data ingestion from NYC Weather API...")

        start_date, end_date = get_dates_from_context(context["data_interval_end"])
        params = {
            "start_date": start_date.split("T")[0],
            "end_date": start_date.split("T")[0],
        }
        try:
            weather_data = ExtractWeatherNyc().extract_weather_data(
                WEATHER_API_URL, params
            )
            weather_data = pd.DataFrame(
                {
                    "date": weather_data["daily"]["time"],
                    "temperature_2m_max": weather_data["daily"]["temperature_2m_max"],
                    "temperature_2m_min": weather_data["daily"]["temperature_2m_min"],
                }
            )

            load_df_to_postgres(weather_data, WEATHER_TABLE, BRONZE_SCHEMA)

            logger.info("Weather data ingestion completed successfully.")
        except Exception as e:
            logger.error("Weather data ingestion failed: %s", e)
            raise

    ingest_311_data()
    ingest_weather_data()


nyc_raw_ingestion()
