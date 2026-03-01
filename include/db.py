import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from include.constants import POSTGRES_CONN_ID


def get_pg_engine(conn_id: str = POSTGRES_CONN_ID):
    """Return a SQLAlchemy engine from an Airflow Postgres connection."""
    return PostgresHook(postgres_conn_id=conn_id).get_sqlalchemy_engine()


def load_df_to_postgres(
    df: pd.DataFrame,
    table: str,
    schema: str,
    conn_id: str = POSTGRES_CONN_ID,
    if_exists: str = "append",
) -> None:
    """Write a DataFrame to a Postgres table via SQLAlchemy."""
    engine = get_pg_engine(conn_id)
    df.to_sql(table, engine, schema=schema, if_exists=if_exists, index=False)
