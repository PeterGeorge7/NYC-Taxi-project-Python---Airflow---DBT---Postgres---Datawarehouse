from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

from include.constants import POSTGRES_CONN_ID
from include.custom_logging import get_logger


@dag(start_date=datetime(2026, 1, 1), schedule="@once", catchup=False)
def check_for_db():

    @task()
    def check_db():
        logger = get_logger("check_db")
        logger.info("Checking connection to PostgreSQL database...")
        try:
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            pg_hook.get_conn()
            logger.info("Connection to PostgreSQL database successful.")
        except Exception as e:
            logger.error("Failed to connect to PostgreSQL database: %s", e)
            raise

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        sql="""
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE SCHEMA IF NOT EXISTS silver;
        CREATE SCHEMA IF NOT EXISTS gold;
        """,
        conn_id=POSTGRES_CONN_ID,
    )

    create_bronze_tables = SQLExecuteQueryOperator(
        task_id="create_bronze_tables",
        sql="""
        CREATE TABLE IF NOT EXISTS bronze.taxi_data (
            unique_key TEXT,
            created_date TEXT,
            closed_date TEXT,
            agency TEXT,
            agency_name TEXT,
            complaint_type TEXT,
            descriptor TEXT,
            location_type TEXT,
            incident_zip TEXT,
            incident_address TEXT,
            street_name TEXT,
            cross_street_1 TEXT,
            cross_street_2 TEXT,
            intersection_street_1 TEXT,
            intersection_street_2 TEXT,
            address_type TEXT,
            city TEXT,
            landmark TEXT,
            status TEXT,
            resolution_description TEXT,
            resolution_action_updated_date TEXT,
            community_board TEXT,
            council_district TEXT,
            police_precinct TEXT,
            bbl TEXT,
            borough TEXT,
            x_coordinate_state_plane TEXT,
            y_coordinate_state_plane TEXT,
            open_data_channel_type TEXT,
            park_facility_name TEXT,
            park_borough TEXT,
            latitude TEXT,
            longitude TEXT
        );
        
        CREATE TABLE IF NOT EXISTS bronze.weather_data (
            date TEXT,
            temperature_2m_max TEXT,
            temperature_2m_min TEXT
        );
        """,
        conn_id=POSTGRES_CONN_ID,
    )

    check_db() >> create_schema >> create_bronze_tables


check_for_db()
