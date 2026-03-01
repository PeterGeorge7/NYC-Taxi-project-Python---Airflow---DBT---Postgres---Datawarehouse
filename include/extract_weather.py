from include.base_extractor import BaseExtractor


class ExtractWeatherNyc(BaseExtractor):
    """Extracts weather data from Open-Meteo (no auth required)."""

    def extract_weather_data(self, url: str, params: dict = None):
        json_response = self._get_json(url, params=params)
        return self._json_to_dataframe(json_response)
