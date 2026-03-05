WITH ranked_data AS (
    SELECT *,
    ROW_NUMBER() OVER (
            PARTITION BY date 
            ORDER BY date DESC
        ) as row_num 
    FROM {{ source('bronze_layer', 'weather_data') }}
),
deduplicated AS (
    SELECT * FROM ranked_data WHERE row_num = 1
)
SELECT 
    date::date,
    temperature_2m_max::float AS max_temperature,
    temperature_2m_min::float AS min_temperature
FROM deduplicated