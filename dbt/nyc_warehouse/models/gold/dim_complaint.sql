{{ config(
    materialized='table',
    schema='gold',
    alias='dim_complaint'
) }}

WITH unique_complaints AS (
    SELECT DISTINCT
        complaint_type,
        descriptor
    FROM  {{ ref('stg_taxi_data') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['complaint_type', 'descriptor']) }} AS complaint_id,
    complaint_type,
    descriptor
FROM unique_complaints