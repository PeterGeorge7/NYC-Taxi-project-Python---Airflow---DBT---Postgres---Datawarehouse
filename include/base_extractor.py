import requests
import pandas as pd


class BaseExtractor:
    """
    Base class for all API extractors.
    """

    def _get_json(self, url: str, params: dict = None, auth: tuple = None) -> dict:
        response = requests.get(url, params=params, auth=auth)
        if response.status_code == 200:
            return response.json()
        raise Exception(
            f"API request failed with status code {response.status_code}: {response.text}"
        )

    def _json_to_dataframe(self, data) -> pd.DataFrame:
        return pd.DataFrame(data)
