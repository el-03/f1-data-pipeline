import sys
import argparse
from typing import Dict
from datetime import datetime

import psycopg2

from metadata import MetadataManager
from api_client import JolpicaAPIClient

from loaders import (
    CircuitLoader,
    SeasonLoader,
    TeamLoader,
    DriverLoader,
    RoundLoader,
    SessionLoader,
    TeamDriverLoader,
    SprintResultLoader,
    QualifyingResultLoader,
    RaceResultLoader,
    DriverChampionshipLoader,
    TeamChampionshipLoader
)

from config import (
    USER, PASSWORD, HOST, PORT, DBNAME,
    LOAD_MODES,
    CURRENT_SEASON,
    TABLES,
    TRIGGERED_BY,
    WORKFLOW_RUN_ID
)


class F1Pipeline:
    """F1 pipeline orchestrator"""

    def __init__(self, conn, api_client, metadata_manager):
        self.conn = conn
        self.api = api_client
        self.metadata = metadata_manager

        # Initialize all loaders

        self.loaders = {
            'circuit': CircuitLoader(conn, api_client, metadata_manager),
            'season': SeasonLoader(conn, api_client, metadata_manager),
            'team': TeamLoader(conn, api_client, metadata_manager),
            'driver': DriverLoader(conn, api_client, metadata_manager),
            'round': RoundLoader(conn, api_client, metadata_manager),
            'session': SessionLoader(conn, api_client, metadata_manager),
            'team_driver': TeamDriverLoader(conn, api_client, metadata_manager),
            'qualifying_result': QualifyingResultLoader(conn, api_client, metadata_manager),
            'sprint_result': SprintResultLoader(conn, api_client, metadata_manager),
            'race_result': RaceResultLoader(conn, api_client, metadata_manager),
            'driver_championship': DriverChampionshipLoader(conn, api_client, metadata_manager),
            'team_championship': TeamChampionshipLoader(conn, api_client, metadata_manager),
        }

    def run_mode(self, mode: str, year: int = CURRENT_SEASON, force: bool = False) -> Dict:
        """
        Run pipeline in specific mode

        Args:
            mode: Loading mode ('pre_season', 'post_race', etc.)
            year: Season year to process
            force: Force loading even if not needed

        Returns:
            Dict with results summary
        """
        print(f"\n{'=' * 70}")
        print(f"F1 DATA PIPELINE - MODE: {mode.upper()}")
        print(f"{'=' * 70}\n")
        print(f"Triggered by: {TRIGGERED_BY}")
        if WORKFLOW_RUN_ID:
            print(f"Workflow run: {WORKFLOW_RUN_ID}")
        print()

        # Get tables for this mode
        tables_to_load = LOAD_MODES.get(mode, [])

        if not tables_to_load:
            print(f"❌ Unknown mode: {mode}")
            print(f"Available modes: {', '.join(LOAD_MODES.keys())}")
            return {'success': False, 'error': 'Unknown mode'}

        print(f"📋 Tables to process: {', '.join(tables_to_load)}\n")

        # Track results
        results = {
            'mode': mode,
            'started_at': datetime.now(),
            'tables_processed': 0,
            'tables_succeeded': 0,
            'tables_failed': 0,
            'tables_skipped': 0,
            'details': []
        }

        raw_zip = None
        if mode in ["pre_season"]:
            print("📦 Downloading preseason dump...")
            raw_zip = self.api.get_raw_zip()
            print("✅ ZIP file downloaded successfully\n")

        # Process each table
        for table_name in tables_to_load:
            result = self._process_table(table_name, year, mode, force, raw_zip=raw_zip)
            results['details'].append(result)
            results['tables_processed'] += 1

            if result['status'] == 'success':
                results['tables_succeeded'] += 1
            elif result['status'] == 'failed':
                results['tables_failed'] += 1
            elif result['status'] == 'skipped':
                results['tables_skipped'] += 1

        results['completed_at'] = datetime.now()
        results['duration_seconds'] = (results['completed_at'] - results['started_at']).seconds

        # Print summary
        self._print_summary(results)

        return results

    def run_table(self, table_name: str, year: int = CURRENT_SEASON, round_num: int = None) -> bool:
        """
        Run a specific table load

        Args:
            table_name: Name of table to load
            year: Season year
            round_num: Specific round (optional)

        Returns:
            True if successful
        """
        print(f"\n{'=' * 70}")
        print(f"🔄 Loading {table_name}")
        print(f"{'=' * 70}\n")

        # Check if table exists
        if table_name not in self.loaders:
            print(f"❌ Unknown table: {table_name}")
            print(f"Available tables: {', '.join(self.loaders.keys())}")
            return False

        # Get loader
        loader = self.loaders[table_name]

        # Prepare kwargs
        kwargs = {'year': year}
        if round_num:
            kwargs['round_num'] = round_num

        # Run
        success = loader.run(**kwargs)

        if success:
            print(f"\n✅ Successfully loaded {table_name}")
        else:
            print(f"\n❌ Failed to load {table_name}")

        return success

    def _process_table(self, table_name: str, year: int, mode: str, force: bool, **kwargs) -> Dict:
        """
        Process a single table

        Returns:
            Dict with table processing result
        """
        result = {
            'table': table_name,
            'status': 'pending',
            'records': 0,
            'duration': 0,
            'error': None
        }

        start_time = datetime.now()

        try:
            # Check if we should load this table
            should_load = force or self.metadata.should_load(table_name, year)

            if not should_load:
                print(f"⏭️  Skipping {table_name} (not needed based on strategy)")
                result['status'] = 'skipped'
                return result

            # Get the loader
            loader = self.loaders.get(table_name)
            if not loader:
                print(f"❌ No loader found for {table_name}")
                result['status'] = 'failed'
                result['error'] = 'No loader available'
                return result

            # Determine what to pass to loader
            kwargs['year'] = year

            # For post-race modes, get the next round to load
            if mode in ['post_race']:
                next_round = self.metadata.get_next_round_to_load(table_name, year)
                if next_round:
                    kwargs['round_num'] = next_round
                    print(f"📍 Loading {table_name} for round {next_round}")
                else:
                    print(f"ℹ️  All rounds already loaded for {table_name}")
                    result['status'] = 'skipped'
                    return result

            # Run the loader
            success = loader.run(**kwargs)

            if success:
                result['status'] = 'success'
                # Get record count from metadata
                watermark = self.metadata.get_watermark(table_name)
                result['records'] = watermark.get('total_records', 0)
            else:
                result['status'] = 'failed'
                result['error'] = 'Loader returned False'

        except Exception as e:
            print(f"❌ Error processing {table_name}: {str(e)}")
            result['status'] = 'failed'
            result['error'] = str(e)

        finally:
            result['duration'] = (datetime.now() - start_time).seconds

        return result

    def _print_summary(self, results: Dict):
        """Print execution summary"""
        print(f"\n{'=' * 70}")
        print(f"📊 EXECUTION SUMMARY")
        print(f"{'=' * 70}\n")

        print(f"Mode: {results['mode']}")
        print(f"Duration: {results['duration_seconds']}s")
        print(f"Tables Processed: {results['tables_processed']}")
        print(f"  ✅ Succeeded: {results['tables_succeeded']}")
        print(f"  ❌ Failed: {results['tables_failed']}")
        print(f"  ⏭️  Skipped: {results['tables_skipped']}")
        print()

        # Details
        if results['details']:
            print("📋 Details:")
            for detail in results['details']:
                status_emoji = {
                    'success': '✅',
                    'failed': '❌',
                    'skipped': '⏭️'
                }.get(detail['status'], '❓')

                table = detail['table']
                records = detail['records']
                duration = detail['duration']

                print(f"  {status_emoji} {table:25s} | {records:6d} records | {duration:3d}s")

                if detail.get('error'):
                    print(f"     Error: {detail['error']}")
            print()

        # Exit status
        if results['tables_failed'] == 0:
            print("🎉 Pipeline completed successfully!")
        else:
            print("⚠️  Pipeline completed with failures")

        print(f"\n{'=' * 70}\n")


def main():
    """Main entry point"""

    # Parse arguments
    parser = argparse.ArgumentParser(
        description='F1 Data Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run pre-season load
  python main_claude.py --mode pre_season

  # Run post-race load
  python main_claude.py --mode post_race --year 2024

  # Load specific table
  python main_claude.py --table driver --year 2024

  # Load specific round
  python main_claude.py --table driver_championship --year 2024 --round 18

  # Check pipeline health
  python main_claude.py --health

  # Force reload
  python main_claude.py --mode pre_season --force
        """
    )

    parser.add_argument(
        '--mode',
        choices=list(LOAD_MODES.keys()),
        help='Loading mode (pre_season, post_race, etc.)'
    )

    parser.add_argument(
        '--table',
        choices=list(TABLES.keys()),
        help='Load specific table'
    )

    parser.add_argument(
        '--year',
        type=int,
        default=CURRENT_SEASON,
        help=f'Season year (default: {CURRENT_SEASON})'
    )

    parser.add_argument(
        '--round',
        type=int,
        help='Specific round number'
    )

    args = parser.parse_args()

    # Validate arguments
    if not any([args.mode, args.table, args.year, args.round]):
        parser.print_help()
        sys.exit(1)

    # Connect to database
    print("🔌 Connecting to database...")
    try:
        conn = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )
        print("✅ Connected successfully\n")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        sys.exit(1)

    # Initialize components
    api_client = JolpicaAPIClient()
    metadata = MetadataManager(conn)
    pipeline = F1Pipeline(conn, api_client, metadata)

    # Execute command
    try:
        if args.table:
            # Load specific table
            success = pipeline.run_table(
                args.table,
                year=args.year,
                round_num=args.round
            )
            sys.exit(0 if success else 1)

        elif args.mode:
            # Run mode
            results = pipeline.run_mode(args.mode)

            # Exit with appropriate code
            if results['tables_failed'] == 0:
                sys.exit(0)
            else:
                sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        sys.exit(130)

    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # Cleanup
        conn.close()
        api_client.close()


if __name__ == "__main__":
    main()
