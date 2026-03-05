from datetime import timedelta
import json
import pandas as pd


def stringfy_complex_json(taxi_data):
    """
    This function takes a DataFrame and converts any columns that contain complex JSON structures (like dictionaries or lists) into string format.
    """
    for col in taxi_data.columns:
        taxi_data[col] = taxi_data[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
        )
    return taxi_data


def get_only_needed_columns(taxi_data):
    expected_columns = [
        "unique_key",
        "created_date",
        "closed_date",
        "agency",
        "agency_name",
        "complaint_type",
        "descriptor",
        "location_type",
        "incident_zip",
        "incident_address",
        "street_name",
        "cross_street_1",
        "cross_street_2",
        "intersection_street_1",
        "intersection_street_2",
        "address_type",
        "city",
        "landmark",
        "status",
        "resolution_description",
        "resolution_action_updated_date",
        "community_board",
        "council_district",
        "police_precinct",
        "bbl",
        "borough",
        "x_coordinate_state_plane",
        "y_coordinate_state_plane",
        "open_data_channel_type",
        "park_facility_name",
        "park_borough",
        "latitude",
        "longitude",
    ]

    # 2. Find which of these columns actually exist in today's API response
    columns_to_keep = [col for col in expected_columns if col in taxi_data.columns]

    # 3. Drop all the random extra columns the API sent (like 'descriptor_2')
    taxi_data = taxi_data[columns_to_keep]

    return taxi_data


def get_dates_from_context(end_date_obj):
    start_date_obj = end_date_obj - timedelta(days=1)

    start_date = start_date_obj.strftime("%Y-%m-%dT00:00:00")
    end_date = end_date_obj.strftime("%Y-%m-%dT00:00:00")

    return start_date, end_date
