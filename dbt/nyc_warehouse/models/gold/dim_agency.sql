{{ config(
    materialized='table',
    schema='gold',
    alias='dim_agency'
) }}

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_taxi_data') }}
),
unique_agencies AS (
    SELECT DISTINCT
        agency,
        agency_name
    FROM staging_data
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['agency', 'agency_name'])}} AS agency_id,
    agency,
    agency_name
FROM unique_agencies