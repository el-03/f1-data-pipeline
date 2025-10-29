from typing import Optional, Dict, Any, List

from config import SCHEMA, SCHEMA_METADATA


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

        if last_round is None or last_season < current_season :
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