{{ config(
    materialized='table',
    schema='gold',
    alias='fct_heating_weather'
) }}

SELECT
    stg.unique_key,
    {{ dbt_utils.generate_surrogate_key(['stg.agency', 'stg.agency_name'])}} AS agency_id,
    {{ dbt_utils.generate_surrogate_key(['stg.complaint_type', 'stg.descriptor']) }} AS complaint_id,
    stg.status,
    {{ dbt_utils.generate_surrogate_key([
        'incident_address', 
        'city', 
        'borough', 
        'incident_zip'
    ]) }} AS location_id,
    stg.open_data_channel_type,
    stg.park_facility_name,
    stg.created_at,
    stg.closed_at,
    weather.max_temperature,
    weather.min_temperature,
    ROUND((EXTRACT(EPOCH FROM (stg.closed_at - stg.created_at)) / 3600.0)::NUMERIC, 2) AS hours_to_close

FROM {{ ref('stg_taxi_data') }} as stg

LEFT JOIN {{ref("stg_weather_data")}} as weather
ON weather.date = stg.created_at::date