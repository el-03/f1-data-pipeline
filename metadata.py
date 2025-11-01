import psycopg2
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from config import LoadStrategy, TABLES, SCHEMA, SCHEMA_METADATA


class MetadataManager:
    def __init__(self, connection):
        self.connection = connection

    def start_sync(self, entity_name: str) -> int:
        cursor = self.connection.cursor()

        try:
            # Update status to 'running'
            cursor.execute(f"""
                UPDATE {SCHEMA_METADATA}.sync_status
                SET status = 'running', 
                    last_updated = NOW()
                WHERE entity_name = %s
            """, (entity_name,))

            # Insert log entry
            cursor.execute(f"""
                INSERT INTO {SCHEMA_METADATA}.sync_log 
                  (entity_name, status, sync_timestamp)
                VALUES (%s, 'running', NOW())
                RETURNING id
            """, (entity_name,))

            log_id = cursor.fetchone()[0]
            self.connection.commit()

            return log_id

        except Exception as e:
            self.connection.rollback()
            raise e

        finally:
            cursor.close()

    def complete_sync(self,
                      entity_name: str,
                      log_id: int,
                      success: bool,
                      records_affected: int = 0,
                      duration_seconds: int = 0,
                      error_message: Optional[str] = None,
                      watermark: Optional[Dict[str, Any]] = None):

        cursor = self.connection.cursor()

        try:
            status = 'success' if success else 'failed'

            # Update sync_log
            cursor.execute(f"""
                UPDATE {SCHEMA_METADATA}.sync_log
                SET status = %s,
                    records_affected = %s,
                    duration_seconds = %s,
                    error_message = %s
                WHERE id = %s
            """, (status, records_affected, duration_seconds, error_message, log_id))

            if success:
                # Build update query dynamically
                update_parts = [
                    "status = 'success'",
                    "last_successful_sync = NOW()",
                    "last_updated = NOW()",
                    "total_records = total_records + %s",
                    "error_message = NULL"
                ]
                params = [records_affected]

                # Add watermark updates if provided
                if watermark:
                    if 'season_year' in watermark and watermark['season_year']:
                        update_parts.append("last_season_year = %s")
                        params.append(watermark['season_year'])
                    if 'round_number' in watermark and watermark['round_number']:
                        update_parts.append("last_round_number = %s")
                        params.append(watermark['round_number'])

                params.append(entity_name)

                update_query = f"""
                    UPDATE {SCHEMA_METADATA}.sync_status
                    SET {', '.join(update_parts)}
                    WHERE entity_name = %s
                """

                cursor.execute(update_query, params)
            else:
                # Update on failure
                cursor.execute(f"""
                    UPDATE {SCHEMA_METADATA}.sync_status
                    SET status = 'failed',
                        last_updated = NOW(),
                        error_message = %s
                    WHERE entity_name = %s
                """, (error_message, entity_name))

            self.connection.commit()

        except Exception as e:
            self.connection.rollback()
            raise e
        finally:
            cursor.close()

    # ========================================
    # WATERMARK MANAGEMENT
    # ========================================

    def get_watermark(self, entity_name: str) -> Optional[dict]:
        cursor = self.connection.cursor()

        try:
            cursor.execute(f"""
                SELECT last_season_year, 
                       last_round_number, 
                       last_successful_sync,
                       total_records
                FROM {SCHEMA_METADATA}.sync_status
                WHERE entity_name = %s
            """, (entity_name,))

            result = cursor.fetchone()

            if result:
                return {
                    'season_year': result[0],
                    'round_number': result[1],
                    'last_sync': result[2],
                    'total_records': result[3]
                }

            # Return empty watermark if entity never loaded
            return {
                'last_sync': None,
                'total_records': 0
            }

        finally:
            cursor.close()

    def get_next_round_to_load(self, entity_name: str, current_season: int) -> Optional[int]:
        """
        Get the next round number that needs to be loaded

        Args:
            entity_name: Table name
            current_season: Current season year

        Returns:
            Next round number or None if all loaded

        Example:
            next_round = metadata.get_next_round_to_load('driver_championship', 2024)
            if next_round:
                api.get_driver_standings(2024, next_round)
        """
        watermark = self.get_watermark(entity_name)
        last_season = watermark.get('season_year')
        last_round = watermark.get('round_number')

        if last_round is None or last_season < current_season:
            return 1  # Start from round 1

        # Check if there are more rounds in the calendar
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"""
                SELECT MAX(number)
                FROM {SCHEMA}.round
                WHERE EXTRACT(YEAR FROM date) = %s
            """, (current_season,))

            result = cursor.fetchone()
            max_round = result[0] if result and result[0] else 0

            if last_round < max_round:
                return last_round + 1

            return None  # All rounds loaded

        finally:
            cursor.close()

    # ========================================
    # LOADING STRATEGY
    # ========================================

    def should_load(self, entity_name: str, current_season: int) -> bool:
        """
        Check if table needs loading based on its strategy

        This implements the smart loading logic that prevents unnecessary API calls.
        Uses the race calendar (round table) to determine when races actually happened.

        Args:
            entity_name: Name of the table
            current_season: Current F1 season year

        Returns:
            True if table should be loaded, False if it can be skipped

        Example:
            if metadata.should_load('circuit'):
                # Load circuits (only if not loaded this year)
            else:
                # Skip, we already have 2024 circuits
        """
        config = TABLES.get(entity_name)
        if not config:
            return False

        watermark = self.get_watermark(entity_name)
        last_sync = watermark.get('last_sync')

        # Never loaded before - definitely load it
        if not last_sync:
            return True

        # PRE_SEASON: Load if we haven't loaded this season yet
        if config.strategy == LoadStrategy.PRE_SEASON:
            last_season = watermark.get('season_year')
            return last_season is None or last_season < current_season

        # POST_RACE: Load after each race
        if config.strategy == LoadStrategy.POST_RACE:
            # Check if there was a sprint race since last sync
            if entity_name == 'sprint_result':
                return self._was_there_sprint_since_last_sync(current_season, last_sync)
            # Check if there was a race since last sync
            return self._was_there_race_since_last_sync(current_season, last_sync)

        # Default: load it
        return True

    def _was_there_race_since_last_sync(self, season_year: int, last_sync: datetime, buffer_days: int = 3) -> bool:
        """
        Check if there was a race X days ago (for championship standings with penalty window)

        Args:
            season_year: Season year to check
            last_sync: When we last synced
            buffer_days: Days to wait after race (default 3 for penalties)

        Returns:
            True if there was a race buffer_days+ ago since last sync
        """
        cur = self.connection.cursor()

        try:
            # Get the most recent race that happened at least buffer_days ago
            cur.execute(f"""
                   SELECT date, number
                   FROM {SCHEMA}.round
                   WHERE EXTRACT(YEAR FROM date) = %s
                     AND date <= CURRENT_DATE - INTERVAL '%s days'
                     AND date IS NOT NULL
                   ORDER BY date DESC
                   LIMIT 1;
               """, (season_year, buffer_days))

            result = cur.fetchone()

            if not result:
                # No races old enough yet
                return False

            race_date, race_number = result

            # Convert to datetime
            if isinstance(race_date, datetime):
                race_datetime = race_date
            else:
                race_datetime = datetime.combine(race_date, datetime.min.time())

            # Check if this race is newer than our last sync
            race_happened_after_sync = race_datetime > (last_sync - timedelta(days=1))

            return race_happened_after_sync

        finally:
            cur.close()

    def _was_there_sprint_since_last_sync(self, season_year: int, last_sync: datetime, buffer_days: int = 3) -> bool:
        """
        Check if there was a sprint race since last sync

        Note: Not all races have sprints. Would need additional data to determine
        which races are sprint weekends. For now, we'll check if sprint data exists.

        Args:
            season_year: Season year to check
            last_sync: When we last synced

        Returns:
            True if there might be sprint data to load
        """
        cur = self.connection.cursor()

        try:
            # Get the most recent race that happened at least buffer_days ago
            cur.execute(f"""
                SELECT r.date AS race_date, r.number AS race_number
                FROM {SCHEMA}.session s
                INNER JOIN {SCHEMA}.round r ON r.id = s.round_id
                WHERE s.type = 'SR' 
                    AND EXTRACT(YEAR FROM r.date) = %s 
                    AND r.date <= CURRENT_DATE - INTERVAL '%s days'
                ORDER BY r.date DESC
                LIMIT 1;
               """, (season_year, buffer_days))

            result = cur.fetchone()

            if not result:
                # No races old enough yet
                return False

            race_date, race_number = result

            # Convert to datetime
            if isinstance(race_date, datetime):
                race_datetime = race_date
            else:
                race_datetime = datetime.combine(race_date, datetime.min.time())

            # Check if this race is newer than our last sync
            race_happened_after_sync = race_datetime > (last_sync - timedelta(days=1))

            return race_happened_after_sync

        finally:
            cur.close()
