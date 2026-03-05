{{ config(
    materialized='table',
    schema='gold',
    alias='dim_location'
) }}

WITH staging_data AS (
    SELECT * FROM {{ ref('stg_taxi_data') }}
),

unique_locations AS (
    SELECT DISTINCT
        incident_address,
        street_name,
        city,
        borough,
        incident_zip,
        latitude,
        longitude,
        community_board,
        council_district,
        police_precinct,
        bbl,
        ROW_NUMBER() OVER (
            PARTITION BY
                incident_address, city, borough, incident_zip
            ORDER BY created_at DESC
        ) as rn
    FROM staging_data
)

SELECT
    -- Generate a unique hash ID for this specific location
    {{ dbt_utils.generate_surrogate_key([
        'incident_address', 
        'city', 
        'borough', 
        'incident_zip'
    ]) }} AS location_id,
    
    incident_address,
    street_name,
    city,
    borough,
    incident_zip,
    latitude,
    longitude,
    community_board,
    council_district,
    police_precinct,
    bbl
FROM unique_locations
WHERE rn = 1