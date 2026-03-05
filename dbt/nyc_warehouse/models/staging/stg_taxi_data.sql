WITH stg AS (
    SELECT *,
    ROW_NUMBER() OVER (
            PARTITION BY unique_key 
            ORDER BY created_date DESC
        ) as row_num 
    FROM {{ source('bronze_layer', 'taxi_data') }}
),
deduplicated AS (
    SELECT * FROM stg WHERE row_num = 1
)

SELECT 
    -- Keep IDs and categorical data as TEXT
    unique_key,
    TRIM(UPPER(agency)) AS agency,
    TRIM(INITCAP(agency_name)) AS agency_name,
    
    -- STANDARDIZATION 1: Force core text to uppercase
    TRIM(UPPER(complaint_type)) AS complaint_type,
    TRIM(UPPER(descriptor)) AS descriptor,
    TRIM(UPPER(status)) AS status,
    
    -- STANDARDIZATION 2: Handle NULLs in critical grouping columns
    COALESCE(TRIM(UPPER(borough)), 'UNSPECIFIED') AS borough,
    COALESCE(TRIM(UPPER(location_type)), 'UNSPECIFIED') AS location_type,
    COALESCE(TRIM(UPPER(city)), 'UNSPECIFIED') AS city,

    -- STANDARDIZATION 3: Clean address-related fields
    COALESCE(TRIM(incident_zip), 'UNKNOWN') AS incident_zip,
    COALESCE(TRIM(UPPER(incident_address)), 'UNKNOWN') AS incident_address,
    COALESCE(TRIM(UPPER(street_name)), 'UNKNOWN') AS street_name,
    COALESCE(TRIM(UPPER(cross_street_1)), 'UNKNOWN') AS cross_street_1,
    COALESCE(TRIM(UPPER(cross_street_2)), 'UNKNOWN') AS cross_street_2,
    COALESCE(TRIM(UPPER(intersection_street_1)), 'UNKNOWN') AS intersection_street_1,
    COALESCE(TRIM(UPPER(intersection_street_2)), 'UNKNOWN') AS intersection_street_2,
    COALESCE(TRIM(UPPER(address_type)), 'UNSPECIFIED') AS address_type,
    COALESCE(TRIM(UPPER(landmark)), 'NONE') AS landmark,

    -- STANDARDIZATION 4: Clean resolution and district fields
    COALESCE(TRIM(INITCAP(resolution_description)), 'Unresolved') AS resolution_description,
    COALESCE(TRIM(UPPER(community_board)), 'UNSPECIFIED') AS community_board,
    COALESCE(council_district::TEXT, '0') AS council_district,
    COALESCE(police_precinct::TEXT, '0') AS police_precinct,

    -- STANDARDIZATION 5: Clean identifiers and channel info
    COALESCE(TRIM(bbl), 'UNKNOWN') AS bbl,
    COALESCE(TRIM(UPPER(open_data_channel_type)), 'UNKNOWN') AS open_data_channel_type,
    COALESCE(TRIM(UPPER(park_facility_name)), 'UNSPECIFIED') AS park_facility_name,
    COALESCE(TRIM(UPPER(park_borough)), 'UNSPECIFIED') AS park_borough,
    
    -- Cast Dates safely
    created_date::TIMESTAMP AS created_at,
    closed_date::TIMESTAMP AS closed_at,
    
    -- Cast Coordinates safely
    x_coordinate_state_plane::FLOAT,
    y_coordinate_state_plane::FLOAT,
    latitude::FLOAT,
    longitude::FLOAT

FROM deduplicated
WHERE UPPER(complaint_type) IN (
    'HEAT/HOT WATER',
    'NON-RESIDENTIAL HEAT',
    'BOILERS',
    'SNOW OR ICE',
    'PLUMBING',
    'WATER LEAK',
    'WATER SYSTEM',
    'DOOR/WINDOW'
)