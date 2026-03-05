# NYC Heating & Weather Data Platform

### An End-to-End Medallion Architecture Pipeline

This project demonstrates a production-grade data engineering ecosystem that synchronizes real-time NYC 311 heating complaint data with global weather metrics. The platform is designed to identify correlations between freezing temperatures and city-wide utility infrastructure strain.

## System Architecture

The project follows a **Medallion Architecture** (Bronze, Silver, Gold) to ensure data quality and lineage:

1.  **Extraction (Bronze):** A Python-based ingestion engine requests data from the NYC Open Data API (Socrata) and Open-Meteo Weather API.
2.  **Orchestration:** Managed by **Airflow (Astro CLI)** within Docker, utilizing **Data-Aware Scheduling** (Datasets) to decouple extraction from transformation.
3.  **Transformation (Silver):** dbt (data build tool) handles schema enforcement, casting, and advanced deduplication using SQL window functions.
4.  **Modeling (Gold):** A **Kimball Star Schema** optimizes the data for analytical performance, utilizing surrogate keys and dimensional modeling.
5.  **Visualization:** Power BI connects to the Gold layer via a Docker network bridge to provide actionable insights.

---

## Technical Features

### 1. Robust Deduplication Logic

To handle incremental appends and backfills without corrupting data, the Silver layer implements a Partitioned Window Function:

```sql
ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY created_date DESC)
```

This ensures that only the most recent version of a 311 ticket is processed into the warehouse.

### 2. Kimball Star Schema Design

The warehouse is modeled for high-performance BI:

- **Fact Table (`fct_heating_weather`)**: Contains grain-level metrics including a calculated `resolution_time_hours` field.
- **Dimension Tables**: `dim_location`, `dim_agency`, and `dim_complaint` utilize MD5-hashed surrogate keys to eliminate expensive text-based joins.

### 3. Automated Data Documentation

The project includes an automated data catalog and lineage graph generated via dbt docs, providing full transparency of the data's journey from API to Dashboard.

---

## Project Visuals

### Data Lineage Graph

![Lineage Graph]()
_(Location for Screenshot: `reports\dbt_lineage.png`)_

### Analytical Dashboard

![Power BI Dashboard]()
_(Location for Screenshot: `reports/page1.jpg`)_

---

## Technical Reference & Documentation

The following resources were instrumental in the architectural design and troubleshooting of this platform:

### Data Sources

- [NYC Open Data API (311 Service Requests)](https://opendata.cityofnewyork.us/)
- [Open-Meteo Historical Weather API](https://open-meteo.com/)

### Core Technologies & Learning Resources

- [PostgreSQL Window Functions](https://www.postgresql.org/docs/current/functions-window.html)
- [dbt Surrogate Keys](https://github.com/dbt-labs/dbt-utils#generate_surrogate_key)
- [Astronomer (Astro CLI) Documentation](https://www.astronomer.io/docs/)
- [Airflow Data-Aware Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
- [PostgreSQL Data Types](https://www.postgresql.org/docs/current/datatype.html)

---

## How to Run

1.  **Start Environment**: Run `astro dev start` to boot the Airflow and Postgres containers.
2.  **Trigger Ingestion**: Enable `dag_1_extract_311` in the Airflow UI to begin the API backfill.
3.  **Run Transformations**: Once extraction completes, `dag_2_run_dbt` will automatically trigger the Gold layer build.
4.  **View Docs**: Navigate to the dbt directory and run `dbt docs generate && dbt docs serve` to view the data catalog.

---

**Developed by Peter George**
Data Engineering Portfolio Project 2026
