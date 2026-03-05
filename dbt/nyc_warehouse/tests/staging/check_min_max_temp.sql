SELECT *
FROM {{ ref('stg_weather_data') }}
WHERE max_temperature < min_temperature