"""
api_client.py
Wrapper for Jolpica F1 API with retry logic and error handling
"""
from io import BytesIO
from zipfile import ZipFile
import requests
import time
from typing import Dict, Any, Optional
from config import JOLPICA_API_BASE, API_TIMEOUT, API_MAX_RETRIES, API_RETRY_DELAY


class JolpicaAPIError(Exception):
    """Custom exception for API errors"""
    pass


class JolpicaAPIClient:
    """Client for Jolpica F1 API (Ergast-compatible)"""

    def __init__(self, base_url: str = JOLPICA_API_BASE):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'F1-Data-Pipeline/1.0',
            'Accept': 'application/json'
        })

    def _make_request(self,
                      endpoint: str,
                      params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic

        Args:
            endpoint: API endpoint (e.g., '/circuits.json')
            params: Query parameters

        Returns:
            JSON response as dict

        Raises:
            JolpicaAPIError: If request fails after retries
        """
        url = f"{self.base_url}{endpoint}"

        for attempt in range(API_MAX_RETRIES):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=API_TIMEOUT
                )

                # Raise for HTTP errors
                response.raise_for_status()

                return response.json()

            except requests.exceptions.Timeout:
                if attempt == API_MAX_RETRIES - 1:
                    raise JolpicaAPIError(f"Request timed out after {API_MAX_RETRIES} attempts: {url}")
                print(f"⚠️  Timeout on attempt {attempt + 1}, retrying...")
                time.sleep(API_RETRY_DELAY ** attempt)

            except requests.exceptions.HTTPError as e:
                # Don't retry on 404 (no data) or 4xx client errors
                if response.status_code == 404:
                    # Return empty result for 404
                    return {'MRData': {'total': '0', 'RaceTable': {'Races': []}}}

                if 400 <= response.status_code < 500:
                    raise JolpicaAPIError(f"Client error {response.status_code}: {e}")

                # Retry on 5xx server errors
                if attempt == API_MAX_RETRIES - 1:
                    raise JolpicaAPIError(f"Server error after {API_MAX_RETRIES} attempts: {e}")
                print(f"⚠️  Server error on attempt {attempt + 1}, retrying...")
                time.sleep(API_RETRY_DELAY ** attempt)

            except requests.exceptions.RequestException as e:
                if attempt == API_MAX_RETRIES - 1:
                    raise JolpicaAPIError(f"Request failed: {e}")
                print(f"⚠️  Request failed on attempt {attempt + 1}, retrying...")
                time.sleep(API_RETRY_DELAY ** attempt)

        # Should never reach here, but just in case
        raise JolpicaAPIError(f"Failed to make request after {API_MAX_RETRIES} attempts")

    # ========================================
    # RESULTS ENDPOINTS
    # ========================================

    def get_race_results(self, year: int, round_num: int) -> Dict:
        """
        Get race results for a specific race

        Args:
            year: Season year
            round_num: Round number

        Returns:
            Dict with MRData.RaceTable.Races[0].Results
        """
        return self._make_request(f"/{year}/{round_num}/results.json")

    def get_qualifying_results(self, year: int, round_num: int) -> Dict:
        """
        Get qualifying results

        Args:
            year: Season year
            round_num: Round number

        Returns:
            Dict with qualifying results
        """
        return self._make_request(f"/{year}/{round_num}/qualifying.json")

    def get_sprint_results(self, year: int, round_num: int) -> Dict:
        """
        Get sprint race results (if applicable)

        Args:
            year: Season year
            round_num: Round number

        Returns:
            Dict with sprint results or empty if no sprint
        """
        return self._make_request(f"/{year}/{round_num}/sprint.json")

    # ========================================
    # STANDINGS ENDPOINTS
    # ========================================

    def get_driver_standings(self, year: int, round_num: Optional[int] = None) -> Dict:
        """
        Get driver championship standings

        Args:
            year: Season year
            round_num: Specific round (None for final standings)

        Returns:
            Dict with MRData.StandingsTable.StandingsLists[0].DriverStandings
        """
        if round_num:
            return self._make_request(f"/{year}/{round_num}/driverStandings.json")
        return self._make_request(f"/{year}/driverStandings.json")

    def get_constructor_standings(self, year: int, round_num: Optional[int] = None) -> Dict:
        """
        Get constructor championship standings

        Args:
            year: Season year
            round_num: Specific round (None for final standings)

        Returns:
            Dict with MRData.StandingsTable.StandingsLists[0].ConstructorStandings
        """
        if round_num:
            return self._make_request(f"/{year}/{round_num}/constructorStandings.json")
        return self._make_request(f"/{year}/constructorStandings.json")

    def get_raw_zip(self) -> Any:
        info = requests.get("https://api.jolpi.ca/data/dumps/download/").json()
        download_url = info["delayed_dumps"]["csv"]["download_url"]
        resp = requests.get(download_url)
        return ZipFile(BytesIO(resp.content))

    def test_connection(self) -> bool:
        """
        Test API connection

        Returns:
            True if connection works
        """
        try:
            result = self.get_seasons(limit=1)
            return 'MRData' in result
        except JolpicaAPIError:
            return False

    def close(self):
        """Close the session"""
        self.session.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

    def __repr__(self):
        return f"JolpicaAPIClient(base_url='{self.base_url}')"
