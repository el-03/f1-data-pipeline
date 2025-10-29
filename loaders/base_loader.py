"""
loaders/base_loader.py
Base class for all data loaders with common ETL logic
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Dict
import pandas as pd

from config import SCHEMA


class BaseLoader(ABC):
    """
    Base class for all data loaders
    Implements common ETL pattern with metadata tracking
    """

    def __init__(self, conn, api_client, metadata_manager):
        """
        Initialize loader

        Args:
            conn: psycopg2 database connection
            api_client: JolpicaAPIClient instance
            metadata_manager: MetadataManager instance
        """
        self.conn = conn
        self.api = api_client
        self.metadata = metadata_manager

    @abstractmethod
    def extract(self, **kwargs) -> Any:
        """
        Extract data from API

        Returns:
            Raw data from API
        """
        pass

    @abstractmethod
    def transform(self, raw_data: Any) -> Any:
        """
        Transform raw API data to database format

        Args:
            raw_data: Raw data from extract()

        Returns:
            List of dicts ready for database insertion
        """
        pass

    @abstractmethod
    def load(self, records: Any) -> int:
        """
        Load transformed records into database

        Args:
            records: Transformed records from transform()

        Returns:
            Number of records inserted/updated
        """
        pass

    @abstractmethod
    def get_entity_name(self) -> str:
        """
        Get the entity name for metadata tracking

        Returns:
            Entity name (e.g., 'circuit', 'driver')
        """
        pass

    def run(self, **kwargs) -> bool:
        """
        Execute full ETL pipeline with metadata tracking

        Args:
            **kwargs: Arguments passed to extract() (e.g., year, round_num)

        Returns:
            True if successful, False otherwise
        """
        raw_data: Any
        entity_name = self.get_entity_name()
        start_time = datetime.now()
        log_id = None

        try:
            # Start tracking
            log_id = self.metadata.start_sync(entity_name)
            print(f"🔄 Starting sync for {entity_name}...")

            # Extract
            print(f"📥 Extracting data...")

            if kwargs.get("raw_zip") is not None:
                print(f"📦 Using shared ZIP for {entity_name}")
                raw_data = kwargs["raw_zip"]
            else:
                print(f"🛜 Fetching from API...")
                raw_data = self.extract(**kwargs)

            if not raw_data:
                print(f"ℹ️  No data to process for {entity_name}")
                self.metadata.complete_sync(
                    entity_name, log_id,
                    success=True,
                    records_affected=0,
                    duration_seconds=0
                )
                return True

            # Transform
            print(f"🔄 Transforming data...")
            records = self.transform(raw_data)

            if not records:
                print(f"ℹ️  No records after transformation for {entity_name}")
                self.metadata.complete_sync(
                    entity_name, log_id,
                    success=True,
                    records_affected=0,
                    duration_seconds=(datetime.now() - start_time).seconds
                )
                return True

            # Load
            if kwargs.get("raw_zip") is not None:
                print(f"📤 Loading records into database...")
            else:
                print(f"📤 Loading {len(records)} records into database...")
            count = self.load(records)

            # Calculate duration
            duration = (datetime.now() - start_time).seconds

            # Extract watermark from kwargs
            watermark = {}
            if 'year' in kwargs:
                watermark['season_year'] = kwargs['year']
            if 'round_num' in kwargs:
                watermark['round_number'] = kwargs['round_num']

            # Complete successfully
            self.metadata.complete_sync(
                entity_name, log_id,
                success=True,
                records_affected=count,
                duration_seconds=duration,
                watermark=watermark if watermark else None
            )

            print(f"✅ {entity_name}: Successfully loaded {count} records in {duration}s")
            return True

        except Exception as e:
            # Calculate duration even on failure
            duration = (datetime.now() - start_time).seconds

            # Log failure
            if log_id:
                self.metadata.complete_sync(
                    entity_name, log_id,
                    success=False,
                    duration_seconds=duration,
                    error_message=str(e)
                )

            print(f"❌ {entity_name}: Failed - {str(e)}")
            return False

    def _build_lookup_maps(self, season_year: int, round_num: int, session_type: str = 'R') -> Dict[str, Dict]:
        """
        Fetch ID reference maps for foreign key resolution.

        Args:
            session_types: Filter sessions by type ('R' = Race, 'Q' = Qualifying, 'S' = Sprint)
        """
        driver_map = pd.read_sql(
            f"SELECT reference, id FROM {SCHEMA}.driver;", self.conn
        ).set_index("reference")["id"].to_dict()

        team_map = pd.read_sql(
            f"SELECT reference, id FROM {SCHEMA}.team;", self.conn
        ).set_index("reference")["id"].to_dict()

        season_map = pd.read_sql(
            f"SELECT year, id FROM {SCHEMA}.season WHERE year = {season_year};", self.conn
        ).set_index("year")["id"].to_dict()

        round_map = pd.read_sql(
            f"SELECT CAST(EXTRACT(YEAR FROM date) AS INT) AS year, number AS round_number, id "
            f"FROM {SCHEMA}.round WHERE CAST(EXTRACT(YEAR FROM date) AS INT) = {season_year} AND number = {round_num};", self.conn
        ).set_index(["year", "round_number"])["id"].to_dict()

        session_map = pd.read_sql(
            f"SELECT round_id, id, number FROM {SCHEMA}.session WHERE type = '{session_type}';", self.conn
        ).set_index("round_id")[["id", "number"]].to_dict(orient="index")

        return {
            "driver_map": driver_map,
            "team_map": team_map,
            "season_map": season_map,
            "round_map": round_map,
            "session_map": session_map,
        }

    @staticmethod
    def convert_time_to_ms(time_str: str) -> int | None:
        if not time_str:
            return None
        try:
            minutes, seconds = time_str.split(":")
            return int((int(minutes) * 60 + float(seconds)) * 1000)
        except ValueError:
            return None

    @staticmethod
    def safe_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
