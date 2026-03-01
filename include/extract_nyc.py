import os

from dotenv import load_dotenv

from include.base_extractor import BaseExtractor

load_dotenv(".env")


class ExtractTaxiNyc(BaseExtractor):
    """Extracts NYC 311 Service Request data (authenticated)."""

    def __init__(self):
        self.api_key_id = os.getenv("API_KEY_ID")
        self.api_key_secret = os.getenv("API_KEY_SECRET")

    def extract_taxi_data(self, url: str, params: dict = None):
        auth = (self.api_key_id, self.api_key_secret)
        json_response = self._get_json(url, params=params, auth=auth)
        return self._json_to_dataframe(json_response)
