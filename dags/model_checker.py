from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from airflow.providers.standard.operators.python import PythonOperator
from include import db


def train_and_predict_by_zip():
    engine = db.get_pg_engine()

    query = (
        query
    ) = """
        WITH daily_zip AS (
            SELECT 
                f.created_at::DATE AS date,
                l.incident_zip,
                f.min_temperature,
                COUNT(f.unique_key) AS daily_complaints
            FROM gold.fct_heating_weather f
            JOIN gold.dim_location l 
                ON f.location_id = l.location_id
            WHERE f.min_temperature IS NOT NULL 
              AND l.incident_zip != 'UNKNOWN'
            GROUP BY 1, 2, 3
        ),
        zip_baseline AS (
            SELECT 
                incident_zip,
                AVG(daily_complaints) AS avg_history
            FROM daily_zip
            GROUP BY 1
        )
        SELECT 
            dz.min_temperature,
            zb.avg_history,
            dz.daily_complaints,
            dz.incident_zip
        FROM daily_zip dz
        JOIN zip_baseline zb 
            ON dz.incident_zip = zb.incident_zip
    """
    print("Fetching hyper-local ZIP code data...")
    df = pd.read_sql(query, engine)

    X = df[["min_temperature", "avg_history"]]
    y = df["daily_complaints"]

    print("Training Random Forest Regressor...")
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)

    prediction_df = df[["incident_zip", "avg_history"]].drop_duplicates().copy()

    prediction_df["min_temperature"] = 4

    print("Predicting tomorrow's complaints per ZIP code...")
    predictions = model.predict(prediction_df[["min_temperature", "avg_history"]])

    prediction_df["predicted_complaints"] = predictions.astype(int)
    prediction_df["prediction_date"] = pd.Timestamp.today().date()

    final_output = prediction_df[
        ["prediction_date", "incident_zip", "min_temperature", "predicted_complaints"]
    ]

    final_output.to_sql(
        "ml_zip_predictions", engine, schema="gold", if_exists="replace", index=False
    )
    print(
        f"Successfully saved predictions for {len(final_output)} ZIP codes to Postgres!"
    )


with DAG(
    dag_id="dag_3_ml_prediction_by_zip",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    predict_task = PythonOperator(
        task_id="run_ml_model_by_zip", python_callable=train_and_predict_by_zip
    )

    predict_task
