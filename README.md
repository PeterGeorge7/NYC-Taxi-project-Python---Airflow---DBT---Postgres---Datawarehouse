# projct steps

Phase 1:
Infrastructure & Environment SetupGoal: Create the "factory" where your data will be processed.Containerization (Docker): Set up a docker-compose.yml file to spin up:PostgreSQL: Your data warehouse.Apache Airflow: Your orchestrator.dbt-core: (Installed locally or via a Docker container) to manage transformations.Database Initialization: \_ Create three distinct schemas in your Postgres database: bronze, silver, and gold.Credential Management:Store your NYC App Token and OpenWeather API Key in Airflow Variables or Connections. Never hardcode them.

Phase 2:
Ingestion Layer (Bronze / Raw)Goal: Move data from the internet into your database without changing it.Airflow DAG - NYC 311 Ingestion:Create a DAG that hits the Socrata API.Real-World Scenario: Implement Incremental Loading. The first run should fetch the last 30 days; every subsequent run should only fetch data where created*date > last_run_date.Airflow DAG - Weather Ingestion:Create a task to fetch the daily high/low temperatures for NYC from the OpenWeather API.Load to Bronze: * Dump the raw JSON/CSV data directly into bronze.raw_311_calls and bronze.raw_weather. Don't worry about data types yet—just get it in there.

Phase 3:
Transformation Layer (Silver / Cleansed)Goal: Use dbt to make the data usable and reliable.dbt Project Setup: Initialize dbt and connect it to your Postgres silver schema.Data Cleaning:Convert timestamps (UTC to EST).Standardize "Borough" names (e.g., "BRONX" vs "The Bronx").Filter the 311 data to only include relevant categories (like "HEAT/HOT WATER").Data Quality (The "Face Real Scenarios" part):Add dbt tests to ensure no duplicate IDs exist and that temperature values aren't impossible (e.g., $150^\circ F$).

Phase 4:
Modeling Layer (Gold / Analytics)Goal: Create a Star Schema designed for the "Heat Prediction" use case.Dimension Tables: Create dim_location (Zip, Borough) and dim_date (Hour, Day, Is_Weekend).Fact Table: Create fct_service_requests. Join your silver 311 data with your silver weather data on the date key.The "Prediction" Model: \* Create a Gold view or table that calculates the Correlation.Logic: Use a Window Function (LAG) to compare today's complaint volume with the temperature from 24 hours ago.

Phase 5:
Visualization & PortfolioGoal: Show what you built.Analysis: Run a final query to find the "Top 5 ZIP Codes at Risk" when the temperature drops below $32^\circ F$ ($0^\circ C$).Documentation: Use dbt docs generate to create a lineage graph showing how data flows from the API to your prediction table.
